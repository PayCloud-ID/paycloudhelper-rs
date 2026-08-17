// Command gen-symmetric-vectors captures Go-produced SNAP-BI symmetric signature
// test vectors for the Rust port (paycloudhelper-rs, crate pc-snapbi).
//
// It calls the REAL exported Go signer, helpers.SymmetricSignatureGen, rather
// than reimplementing HMAC-SHA512 over the string-to-sign. Reimplementing it
// here would make the output a Rust-independent second opinion of nothing: the
// whole value of an oracle vector is that it came from the code in production.
//
// Output goes to a FILE, not stdout. paycloudhelper's init() runs
// InitializeLogger()/LogConfigurationWarnings(), and SymmetricSignatureGen
// itself calls pchelper.LogD at helpers/signature.go:191, all of which write to
// stdout via kataras/golog. Parsing stdout would interleave log lines into the
// JSON.
//
// This program is NOT part of the service build and is not committed to the
// service repo. Run it, copy the JSON into the Rust fixture, remove it:
//
//	go run ./cmd/gen-symmetric-vectors -out /tmp/symmetric_go_vectors.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"paycloud-be-qoinhubinterface-manager/helpers"
	"paycloud-be-qoinhubinterface-manager/structs"
)

type vector struct {
	Name      string `json:"name"`
	Secret    string `json:"secret"`
	Method    string `json:"method"`
	URL       string `json:"url"`
	Token     string `json:"token"`
	Body      string `json:"body"`
	Timestamp string `json:"timestamp"`
	Signature string `json:"signature"`
	Note      string `json:"note"`
}

type fixture struct {
	Oracle     string   `json:"oracle"`
	CapturedAt string   `json:"captured_at"`
	Note       string   `json:"note"`
	Vectors    []vector `json:"vectors"`
}

// Obviously-fake test values only. Nothing here is a real credential.
const (
	secret = "test-secret-key"
	token  = "access-token-abc"
	url    = "/v1.0/qr/qr-mpm-generate"
	method = "POST"
	ts     = "2026-08-17T10:00:00+07:00"
)

func main() {
	out := flag.String("out", "symmetric_go_vectors.json", "output file")
	capturedAt := flag.String("captured-at", "", "YYYY-MM-DD stamp for the fixture")
	flag.Parse()
	if *capturedAt == "" {
		fmt.Fprintln(os.Stderr, "-captured-at is required (do not derive it from the clock)")
		os.Exit(2)
	}

	cases := []struct {
		name string
		body string
		note string
	}{
		{
			name: "compact-body",
			body: `{"partnerReferenceNo":"ORDER-1","amount":{"value":"1000.00","currency":"IDR"}}`,
			note: "Already-compact JSON: raw and minified bytes are identical, so Go's raw hash and " +
				"the Rust port's minified hash MUST agree. This is the vector that proves the live " +
				"outbound signing path matches.",
		},
		{
			name: "pretty-body",
			body: "{\n  \"partnerReferenceNo\": \"ORDER-1\",\n  \"amount\": {\n    \"value\": \"1000.00\"\n  }\n}",
			note: "Pretty-printed JSON: SymmetricSignatureGen hashes the raw whitespace bytes, the " +
				"Rust port hashes the compacted bytes, so these MUST differ. Pins the known " +
				"divergence instead of hiding it.",
		},
		{
			name: "invalid-json-body",
			body: `{invalid}`,
			note: "SymmetricSignatureGen never calls JsonMinify, so it hashes the raw bytes. The " +
				"Rust port's minify fails and falls back to hashing EMPTY (matching Go's " +
				"JsonMinify returning []byte{} on error). These MUST differ.",
		},
		{
			name: "empty-body",
			body: ``,
			note: "Empty body: Go hashes the empty slice. The Rust port's minify also fails on empty " +
				"input and falls back to empty, so both hash sha256(\"\") and MUST agree.",
		},
	}

	f := fixture{
		Oracle: "qoinhub Go helpers.SymmetricSignatureGen (RAW body hash path, " +
			"helpers/signature.go:187-198), called directly via cmd/gen-symmetric-vectors " +
			"inside the paycloud-be-qoinhubinterface-manager module",
		CapturedAt: *capturedAt,
		Note: "Genuine Go oracle vectors. Distinct from symmetric_signatures.json, which is " +
			"Rust self-consistency data for the MINIFIED layout and says so. Go has three " +
			"copies of this string-to-sign and only SymmetricSignatureGen is both exported " +
			"and raw-hashing, so only the raw path can be captured from a real Go entry point; " +
			"services.SignatureService is an unexported method on a private struct.",
	}

	for _, c := range cases {
		sig := helpers.SymmetricSignatureGen(&structs.SymmetricSignature{
			Method:          method,
			Url:             url,
			AccessToken:     token,
			HttpRequestBody: []byte(c.body),
			Timestamp:       ts,
			SecretKey:       secret,
			Identifier:      "vector-capture",
		})
		f.Vectors = append(f.Vectors, vector{
			Name:      c.name,
			Secret:    secret,
			Method:    method,
			URL:       url,
			Token:     token,
			Body:      c.body,
			Timestamp: ts,
			Signature: sig,
			Note:      c.note,
		})
	}

	blob, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, append(blob, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %d vectors to %s\n", len(f.Vectors), *out)
}
