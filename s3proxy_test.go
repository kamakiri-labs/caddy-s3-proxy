package caddys3proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws/awserr"
	caddy "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type jTestCase struct {
	root     string
	path     string
	expected string
}

func TestJoinPath(t *testing.T) {
	testCases := []jTestCase{
		jTestCase{
			root:     "",
			path:     "/foo",
			expected: "/foo",
		},
		jTestCase{
			root:     "",
			path:     "/",
			expected: "/",
		},
		jTestCase{
			root:     "/",
			path:     "/",
			expected: "/",
		},
		jTestCase{
			root:     "/",
			path:     "/foo",
			expected: "/foo",
		},
		jTestCase{
			root:     "/cat",
			path:     "/dog",
			expected: "/cat/dog",
		},
		jTestCase{
			root:     "/cat/",
			path:     "/dog",
			expected: "/cat/dog",
		},
		jTestCase{
			root:     "/cat/",
			path:     "/dog/",
			expected: "/cat/dog/",
		},
		jTestCase{
			root:     "",
			path:     "/dog/",
			expected: "/dog/",
		},
	}
	for _, tc := range testCases {
		r := joinPath(tc.root, tc.path)
		if r != tc.expected {
			t.Errorf("When joining '%s' and '%s' we expected '%s' but got '%s'", tc.root, tc.path, tc.expected, r)
		}
	}
}

func TestFileHidden(t *testing.T) {
	for i, tc := range []struct {
		inputHide []string
		inputPath string
		expect    bool
	}{
		{
			inputHide: nil,
			inputPath: "",
			expect:    false,
		},
		{
			inputHide: []string{".gitignore"},
			inputPath: "/.gitignore",
			expect:    true,
		},
		{
			inputHide: []string{".git"},
			inputPath: "/.gitignore",
			expect:    false,
		},
		{
			inputHide: []string{"/.git"},
			inputPath: "/.gitignore",
			expect:    false,
		},
		{
			inputHide: []string{".git"},
			inputPath: "/.git",
			expect:    true,
		},
		{
			inputHide: []string{".git"},
			inputPath: "/.git/foo",
			expect:    true,
		},
		{
			inputHide: []string{".git"},
			inputPath: "/foo/.git/bar",
			expect:    true,
		},
		{
			inputHide: []string{"/prefix"},
			inputPath: "/prefix/foo",
			expect:    true,
		},
		{
			inputHide: []string{"/foo/*/bar"},
			inputPath: "/foo/asdf/bar",
			expect:    true,
		},
		{
			inputHide: []string{"/foo"},
			inputPath: "/foo",
			expect:    true,
		},
		{
			inputHide: []string{"/foo"},
			inputPath: "/foobar",
			expect:    false,
		},
	} {
		// for Windows' sake
		tc.inputPath = filepath.FromSlash(tc.inputPath)
		for i := range tc.inputHide {
			tc.inputHide[i] = filepath.FromSlash(tc.inputHide[i])
		}

		actual := fileHidden(tc.inputPath, tc.inputHide)
		if actual != tc.expect {
			t.Errorf("Test %d: Is %s hidden in %v? Got %t but expected %t",
				i, tc.inputPath, tc.inputHide, actual, tc.expect)
		}
	}
}

func newS3Client(t *testing.T) *s3.S3 {
	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("Skipping test because AWS_ENDPOINT environment variable is not set.")
	}

	config := aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Endpoint:         aws.String(endpoint),
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		t.Fatal(err)
	}

	return s3.New(sess)
}

func setupTestBucket(t *testing.T, client *s3.S3) string {
	bucketName := fmt.Sprintf(
		"caddy-s3-proxy-testdata-%d-%d",
		time.Now().UnixNano(),
		rand.Int(),
	)
	testDataDir := "testdata"

	_, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if awsErr, isAwsErr := err.(awserr.Error); isAwsErr {
		if awsErr.Code() == s3.ErrCodeBucketAlreadyExists {
			err = nil
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	if err := filepath.Walk(testDataDir, func(p string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		if err != nil {
			return err
		}

		key := strings.TrimPrefix(p, testDataDir)
		contentType := mime.TypeByExtension(filepath.Ext(p))
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		file, err := os.Open(p)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := client.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(bucketName),
			Key:         aws.String(key),
			ContentType: aws.String(contentType),
			Body:        file,
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return bucketName
}

func boolPtr(b bool) *bool { return &b }

// brokenS3Client returns an S3 client pointed at a closed port so any request
// fails fast with a connection error, which the handler maps to a 500. Used to
// exercise the "5xx does not trigger the not_found_page branch" case.
func brokenS3Client() *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String("http://127.0.0.1:1"),
		S3ForcePathStyle: aws.Bool(true),
		MaxRetries:       aws.Int(0),
		Credentials:      credentials.NewStaticCredentials("test", "test", ""),
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
	})
	if err != nil {
		panic(err)
	}
	return s3.New(sess)
}

func TestProxy(t *testing.T) {
	client := newS3Client(t)
	bucketName := setupTestBucket(t, client)

	for _, tc := range []struct {
		name                 string
		proxy                S3Proxy
		method               string
		body                 []byte
		headers              http.Header
		path                 string
		next                 caddyhttp.Handler
		clientOverride       *s3.S3
		expectedCode         int
		expectedHeaders      http.Header
		expectedResponseText string
		expectsEmptyResponse bool
		expectReturnNil      *bool
	}{
		{
			name:                 "can get simple JSON object",
			proxy:                S3Proxy{Bucket: bucketName},
			method:               http.MethodGet,
			path:                 "/test.json",
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo": "bar"}`,
			expectedHeaders: http.Header{
				"Content-Type": []string{"application/json"},
			},
		},
		{
			name:                 "hidden file are not served",
			proxy:                S3Proxy{Bucket: bucketName, Hide: []string{"test.json"}},
			method:               http.MethodGet,
			path:                 "/test.json",
			expectedCode:         http.StatusNotFound,
			expectsEmptyResponse: true,
		},
		{
			name:                 "can't post",
			proxy:                S3Proxy{Bucket: bucketName},
			method:               http.MethodPost,
			path:                 "/cannot-post",
			expectedCode:         http.StatusMethodNotAllowed,
			expectsEmptyResponse: true,
		},
		{
			name:                 "can't delete if not allowed",
			proxy:                S3Proxy{Bucket: bucketName},
			method:               http.MethodDelete,
			path:                 "/cannot-delete",
			expectedCode:         http.StatusMethodNotAllowed,
			expectsEmptyResponse: true,
		},
		{
			name:                 "can delete if allowed",
			proxy:                S3Proxy{Bucket: bucketName, EnableDelete: true},
			method:               http.MethodDelete,
			path:                 "/to-delete.json",
			expectedCode:         http.StatusOK,
			expectsEmptyResponse: true,
		},
		{
			name:                 "can't put if not allowed",
			proxy:                S3Proxy{Bucket: bucketName},
			method:               http.MethodPut,
			path:                 "/cannot-put",
			expectedCode:         http.StatusMethodNotAllowed,
			expectsEmptyResponse: true,
		},
		{
			name:                 "can put if allowed",
			proxy:                S3Proxy{Bucket: bucketName, EnablePut: true},
			method:               http.MethodPut,
			path:                 "/can-put",
			body:                 []byte("some content"),
			expectedCode:         http.StatusOK,
			expectsEmptyResponse: true,
		},
		{
			name:                 "serves index.html",
			proxy:                S3Proxy{Bucket: bucketName, IndexNames: []string{"index.html"}},
			method:               http.MethodGet,
			path:                 "/inner/",
			expectedCode:         http.StatusOK,
			expectedResponseText: "my index.html",
		},
		{
			name:   "returns 304 If-None-Match on index",
			proxy:  S3Proxy{Bucket: bucketName, IndexNames: []string{"index.html"}},
			method: http.MethodGet,
			path:   "/inner/",
			headers: http.Header{
				"If-None-Match": []string{`"44bacca965de5aef310706cc55c4a7b0"`},
			},
			expectedCode:         http.StatusNotModified,
			expectsEmptyResponse: true,
		},
		{
			name:                 "cannot browse",
			proxy:                S3Proxy{Bucket: bucketName},
			method:               http.MethodGet,
			path:                 "/inner/",
			expectedCode:         http.StatusForbidden,
			expectsEmptyResponse: true,
		},
		{
			name:         "returns 404 if not found",
			proxy:        S3Proxy{Bucket: bucketName},
			method:       http.MethodGet,
			path:         "/doesnt-exist",
			expectedCode: http.StatusNotFound,
		},
		{
			name: "returns 404 page if 404 error page is set",
			proxy: S3Proxy{
				Bucket:           bucketName,
				ErrorPages:       map[int]string{404: "_404.txt"},
				DefaultErrorPage: "default_error_page.txt",
			},
			method:               http.MethodGet,
			path:                 "/doesnt-exist",
			expectedCode:         http.StatusNotFound,
			expectedResponseText: `this is 404`,
		},
		{
			name: "returns default page if default error page is set",
			proxy: S3Proxy{
				Bucket:           bucketName,
				DefaultErrorPage: "default_error_page.txt",
			},
			method:               http.MethodGet,
			path:                 "/doesnt-exist",
			expectedCode:         http.StatusNotFound,
			expectedResponseText: `this is a default error page`,
		},
		{
			name:                 "serves not_found_page with forced 404 for a missing key",
			proxy:                S3Proxy{Bucket: bucketName, NotFoundPage: "404.html"},
			method:               http.MethodGet,
			path:                 "/doesnt-exist",
			expectedCode:         http.StatusNotFound,
			expectedResponseText: `<!doctype html><title>Not found</title><h1>site custom 404 page</h1>`,
			expectedHeaders: http.Header{
				"Content-Type": []string{"text/html; charset=utf-8"},
			},
			expectReturnNil: boolPtr(true),
		},
		{
			// A directory with browsing disabled triggers a 403; the not_found_page
			// must still be served with a forced 404 (status forced, not echoed).
			name:                 "forces 404 for a 403 trigger when not_found_page is set",
			proxy:                S3Proxy{Bucket: bucketName, NotFoundPage: "404.html"},
			method:               http.MethodGet,
			path:                 "/inner/",
			expectedCode:         http.StatusNotFound,
			expectedResponseText: `<!doctype html><title>Not found</title><h1>site custom 404 page</h1>`,
			expectReturnNil:      boolPtr(true),
		},
		{
			// not_found_page is set but the page itself is missing: fall through to
			// the next handler (the platform's branded default). The stub writes a
			// distinct status (418) so the assertion also proves the handler wrote
			// nothing to the response before delegating - next owns status + body.
			name:   "falls through to next when not_found_page is missing",
			proxy:  S3Proxy{Bucket: bucketName, NotFoundPage: "no-such-404.html"},
			method: http.MethodGet,
			path:   "/doesnt-exist",
			next: caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
				w.WriteHeader(http.StatusTeapot)
				_, _ = w.Write([]byte("branded fallback"))
				return nil
			}),
			expectedCode:         http.StatusTeapot,
			expectedResponseText: `branded fallback`,
		},
		{
			// A 5xx (here, an unreachable backend) must not trigger the branch; the
			// original status propagates.
			name:                 "does not serve not_found_page for a 5xx",
			proxy:                S3Proxy{Bucket: bucketName, NotFoundPage: "404.html"},
			clientOverride:       brokenS3Client(),
			method:               http.MethodGet,
			path:                 "/doesnt-exist",
			expectedCode:         http.StatusInternalServerError,
			expectsEmptyResponse: true,
			expectReturnNil:      boolPtr(false),
		},
		{
			name:   "returns range",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"Range": []string{"bytes=0-4"},
			},
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo`,
		},
		{
			name:   "returns 200 code If-Match",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Match": []string{`"a38212e01d6f419c9bd303b304a99e9b"`},
			},
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo": "bar"}`,
		},
		{
			name:   "returns 412 If-Match",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Match": []string{`"no good etag"`},
			},
			expectedCode:         http.StatusPreconditionFailed,
			expectsEmptyResponse: true,
		},
		{
			name:   "returns 304 If-None-Match",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-None-Match": []string{`"a38212e01d6f419c9bd303b304a99e9b"`},
			},
			expectedCode:         http.StatusNotModified,
			expectsEmptyResponse: true,
		},
		{
			name:   "returns 200 If-None-Match",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-None-Match": []string{`"no good etag"`},
			},
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo": "bar"}`,
		},
		{
			name:   "returns 200 If-Unmodified-Since",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Unmodified-Since": []string{`Thu, 05 May 2568 07:28:00 GMT`},
			},
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo": "bar"}`,
		},
		{
			name:   "returns 412 If-Unmodified-Since",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Unmodified-Since": []string{`Wed, 21 Oct 2015 07:28:00 GMT`},
			},
			expectedCode:         http.StatusPreconditionFailed,
			expectsEmptyResponse: true,
		},
		{
			name:   "returns 200 If-Modified-Since",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Modified-Since": []string{`Thu, 05 May 2568 07:28:00 GMT`},
			},
			expectedCode:         http.StatusNotModified,
			expectsEmptyResponse: true,
		},
		{
			name:   "returns 412 If-Modified-Since",
			proxy:  S3Proxy{Bucket: bucketName},
			method: http.MethodGet,
			path:   "/test.json",
			headers: http.Header{
				"If-Modified-Since": []string{`Wed, 21 Oct 2015 07:28:00 GMT`},
			},
			expectedCode:         http.StatusOK,
			expectedResponseText: `{"foo": "bar"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != nil {
				body = bytes.NewReader(tc.body)
			}

			req, err := http.NewRequest(tc.method, tc.path, body)
			if err != nil {
				t.Fatal(err)
			}
			repl := caddy.NewReplacer()
			ctx := context.WithValue(req.Context(), caddy.ReplacerCtxKey, repl)
			req = req.WithContext(ctx)
			req.Header = tc.headers

			recorder := httptest.NewRecorder()

			if tc.clientOverride != nil {
				tc.proxy.client = tc.clientOverride
			} else {
				tc.proxy.client = client
			}
			tc.proxy.log = zap.NewExample()

			gotErr := tc.proxy.ServeHTTP(recorder, req, tc.next)

			// Check HTTP status code
			if tc.expectedCode != 0 && recorder.Code != tc.expectedCode {
				t.Errorf("Expected code %d, got %d.", tc.expectedCode, recorder.Code)
			}

			// Check response headers
			respHeaders := recorder.Header()
			for k, v := range tc.expectedHeaders {
				if !reflect.DeepEqual(respHeaders.Values(k), v) {
					t.Errorf("Expected headers %v, got %v.", tc.expectedHeaders, respHeaders.Values(k))
				}
			}

			// Check response body
			if tc.expectedResponseText != "" && tc.expectedResponseText != strings.TrimSpace(recorder.Body.String()) {
				t.Errorf(
					"Expected response text %s, got %s.",
					tc.expectedResponseText,
					recorder.Body.String(),
				)
			}

			// Check if response should be empty
			if tc.expectsEmptyResponse && recorder.Body.Len() != 0 {
				t.Errorf("Expected response body to be empty, got %s.", recorder.Body.String())
			}

			// Check the handler's return value when the case cares about it.
			// A nil return after a full write is what keeps the body intact
			// through a buffering cache.
			if tc.expectReturnNil != nil {
				if *tc.expectReturnNil && gotErr != nil {
					t.Errorf("Expected ServeHTTP to return nil, got %v.", gotErr)
				}
				if !*tc.expectReturnNil && gotErr == nil {
					t.Errorf("Expected ServeHTTP to return a non-nil error, got nil.")
				}
			}
		})
	}
}
