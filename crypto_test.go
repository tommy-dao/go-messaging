package message

import "testing"

func TestAES256GCMCipher_RoundTrip(t *testing.T) {
	c := NewAES256GCMCipher("v1", "super-secret")
	pt := []byte(`{"hello":"world"}`)

	ct, err := c.Encrypt(pt)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if string(ct) == string(pt) {
		t.Error("ciphertext equals plaintext")
	}

	got, err := c.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if string(got) != string(pt) {
		t.Errorf("Decrypt = %s, want %s", got, pt)
	}
}

func TestCipherRegistry_EncryptDecrypt(t *testing.T) {
	c := NewDefaultCipher("secret")
	reg, err := NewCipherRegistry([]MessageCipher{c}, c.Version())
	if err != nil {
		t.Fatalf("NewCipherRegistry failed: %v", err)
	}

	pt := []byte(`{"a":1}`)
	stored, err := reg.Encrypt(pt)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// stored must be a valid JSON scalar (quoted string) so it fits a JSONB column.
	if stored[0] != '"' {
		t.Errorf("stored value is not a JSON string: %s", stored)
	}

	got, err := reg.Decrypt(stored)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if string(got) != string(pt) {
		t.Errorf("Decrypt = %s, want %s", got, pt)
	}
}

func TestCipherRegistry_KeyRotation_OldVersionStillDecrypts(t *testing.T) {
	v1 := NewAES256GCMCipher("v1", "secret-1")
	v2 := NewAES256GCMCipher("v2", "secret-2")

	regV1, err := NewCipherRegistry([]MessageCipher{v1}, "v1")
	if err != nil {
		t.Fatalf("NewCipherRegistry(v1) failed: %v", err)
	}
	pt := []byte(`{"legacy":true}`)
	storedV1, err := regV1.Encrypt(pt)
	if err != nil {
		t.Fatalf("Encrypt(v1) failed: %v", err)
	}

	// Rotate: current version is now v2, but v1 stays registered for old rows.
	regRotated, err := NewCipherRegistry([]MessageCipher{v1, v2}, "v2")
	if err != nil {
		t.Fatalf("NewCipherRegistry(rotated) failed: %v", err)
	}

	got, err := regRotated.Decrypt(storedV1)
	if err != nil {
		t.Fatalf("Decrypt(storedV1) after rotation failed: %v", err)
	}
	if string(got) != string(pt) {
		t.Errorf("Decrypt = %s, want %s", got, pt)
	}

	storedV2, err := regRotated.Encrypt(pt)
	if err != nil {
		t.Fatalf("Encrypt(v2) failed: %v", err)
	}
	got2, err := regRotated.Decrypt(storedV2)
	if err != nil {
		t.Fatalf("Decrypt(storedV2) failed: %v", err)
	}
	if string(got2) != string(pt) {
		t.Errorf("Decrypt = %s, want %s", got2, pt)
	}
}

func TestCipherRegistry_UnknownVersionErrors(t *testing.T) {
	c := NewDefaultCipher("secret")
	reg, err := NewCipherRegistry([]MessageCipher{c}, c.Version())
	if err != nil {
		t.Fatalf("NewCipherRegistry failed: %v", err)
	}
	if _, err := reg.Decrypt([]byte(`"v99.no-such-version:AAAA"`)); err == nil {
		t.Error("Decrypt with unknown version should error")
	}
}

func TestCipherRegistry_RequiresCurrentVersionAmongCiphers(t *testing.T) {
	c := NewDefaultCipher("secret")
	if _, err := NewCipherRegistry([]MessageCipher{c}, "not-registered"); err == nil {
		t.Error("NewCipherRegistry should error when currentVersion is not among ciphers")
	}
}
