package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var certName = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,62})$`)

// writeCerts writes a throwaway CA and a server certificate for each name
// (a DNS name, or an IP address) into dir: ca.pem, <name>.pem and
// <name>-key.pem, keys mode 0600. The CA's key is discarded, so nothing
// more can be signed by it. For a laptop's quickstart, never a network.
func writeCerts(dir string, names []string) error {
	if len(names) == 0 {
		return errors.New("no names")
	}
	for _, n := range names {
		if !certName.MatchString(n) {
			return fmt.Errorf("name %q: lowercase letters, digits, dots and hyphens", n)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bstore quickstart CA (throwaway)"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, "ca.pem"), "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	for i, n := range names {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i) + 2),
			Subject:      pkix.Name{CommonName: n},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.AddDate(1, 0, 0),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if ip := net.ParseIP(n); ip != nil {
			leaf.IPAddresses = []net.IP{ip}
		} else {
			leaf.DNSNames = []string{n}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		kder, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, n+".pem"), "CERTIFICATE", der, 0o644); err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, n+"-key.pem"), "EC PRIVATE KEY", kder, 0o600); err != nil {
			return err
		}
		fmt.Printf("certificate %s\n", n)
	}
	return nil
}

func writePEM(path, typ string, der []byte, mode os.FileMode) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), mode)
}
