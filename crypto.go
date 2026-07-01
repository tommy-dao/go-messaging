package message

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// MessageCipher encrypts/decrypts arbitrary bytes and self-identifies by
// Version. Implement this for AES/KMS/HSM — CipherRegistry only calls these
// two methods and owns the version-prefix framing.
type MessageCipher interface {
	Version() string
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// aes256GCMCipher is the default MessageCipher: AES-256-GCM with
// key = sha256(secret). Ciphertext body layout: iv[12] | tag[16] | ct.
// The cipher.AEAD is built once at construction (aes.NewCipher/cipher.NewGCM
// are non-trivial — key schedule + GHASH table setup) and reused across
// calls; it's safe for concurrent Seal/Open since each call supplies its own
// random nonce and the AEAD holds no per-call mutable state.
type aes256GCMCipher struct {
	version string
	gcm     cipher.AEAD
}

// NewAES256GCMCipher builds a default AES-256-GCM cipher identified by version.
func NewAES256GCMCipher(version, secret string) MessageCipher {
	if secret == "" {
		panic("message: NewAES256GCMCipher: a non-empty secret is required")
	}
	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic("message: NewAES256GCMCipher: " + err.Error())
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic("message: NewAES256GCMCipher: " + err.Error())
	}
	return &aes256GCMCipher{version: version, gcm: gcm}
}

// NewDefaultCipher builds a default AES-256-GCM cipher with version "v0.0.0".
func NewDefaultCipher(secret string) MessageCipher {
	return NewAES256GCMCipher("v0.0.0", secret)
}

func (c *aes256GCMCipher) Version() string { return c.version }

func (c *aes256GCMCipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("message: nonce: %w", err)
	}
	return c.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (c *aes256GCMCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := c.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("message: ciphertext too short")
	}
	nonce, body := ciphertext[:nonceSize], ciphertext[nonceSize:]
	pt, err := c.gcm.Open(nil, nonce, body, nil)
	if err != nil {
		return nil, fmt.Errorf("message: decrypt: %w", err)
	}
	return pt, nil
}

var cipherVersionRegex = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// CipherRegistry routes encrypt/decrypt across a set of versioned ciphers.
// Encrypt always uses the current version and stamps the stored value with
// "<version>:<base64(body)>" as a JSON string (a valid JSONB scalar) so the
// payload/headers columns stay JSONB-typed even when encrypted. Decrypt reads
// the prefix to route to the right cipher, so rotation only requires adding a
// new version to the set and pointing currentVersion at it — old ciphertext
// keeps decrypting via the retained old-version cipher, no schema change, no
// backfill.
type CipherRegistry struct {
	byVersion map[string]MessageCipher
	current   MessageCipher
}

// NewCipherRegistry builds a registry from ciphers, encrypting new writes
// with currentVersion (which must be one of the ciphers).
func NewCipherRegistry(ciphers []MessageCipher, currentVersion string) (*CipherRegistry, error) {
	if len(ciphers) == 0 {
		return nil, fmt.Errorf("message: CipherRegistry: at least one cipher is required")
	}
	byVersion := make(map[string]MessageCipher, len(ciphers))
	for _, c := range ciphers {
		v := c.Version()
		if !cipherVersionRegex.MatchString(v) {
			return nil, fmt.Errorf("message: CipherRegistry: invalid version %q", v)
		}
		if _, dup := byVersion[v]; dup {
			return nil, fmt.Errorf("message: CipherRegistry: duplicate version %q", v)
		}
		byVersion[v] = c
	}
	cur, ok := byVersion[currentVersion]
	if !ok {
		return nil, fmt.Errorf("message: CipherRegistry: currentVersion %q is not among the ciphers", currentVersion)
	}
	return &CipherRegistry{byVersion: byVersion, current: cur}, nil
}

// Encrypt returns the JSON-encoded (quoted) "<version>:<base64>" string ready
// to store directly in a JSONB column.
func (r *CipherRegistry) Encrypt(plaintext []byte) ([]byte, error) {
	ct, err := r.current.Encrypt(plaintext)
	if err != nil {
		return nil, err
	}
	stamped := r.current.Version() + ":" + base64.StdEncoding.EncodeToString(ct)
	return json.Marshal(stamped)
}

// Decrypt reads a JSON-quoted "<version>:<base64>" value (as produced by
// Encrypt, and as stored by the JSONB column) and returns the plaintext.
func (r *CipherRegistry) Decrypt(stored []byte) ([]byte, error) {
	var stamped string
	if err := json.Unmarshal(stored, &stamped); err != nil {
		return nil, fmt.Errorf("message: CipherRegistry: stored value is not a JSON string: %w", err)
	}
	i := strings.IndexByte(stamped, ':')
	if i <= 0 {
		return nil, fmt.Errorf("message: CipherRegistry: ciphertext missing version prefix")
	}
	version, body := stamped[:i], stamped[i+1:]
	c, ok := r.byVersion[version]
	if !ok {
		return nil, fmt.Errorf("message: CipherRegistry: no cipher registered for version %q", version)
	}
	ct, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("message: CipherRegistry: invalid base64 body: %w", err)
	}
	return c.Decrypt(ct)
}
