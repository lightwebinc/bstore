package publish

import (
	"slices"
	"testing"
)

// A closure sent by upload session is only claimed by an entry: the entry
// goes to the origin of each public URL, once, and a provider named twice
// hears once.
func TestEntryReachesUploadProviders(t *testing.T) {
	d := &Delivery{Hosts: []string{"https://host.example.com"}, Providers: []string{"https://b.example.com/"},
		Uploads: []string{"https://a.example.com/store", "https://b.example.com/store"}}
	providers := func(entry bool) []string {
		var out []string
		for _, target := range d.recordTargets("b", entry) {
			if !target.Host {
				out = append(out, target.Base)
			}
		}
		return out
	}
	if got, want := providers(true), []string{"https://b.example.com", "https://a.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("an entry goes to %q, want %q", got, want)
	}
	if got := providers(false); len(got) != 0 {
		t.Fatalf("a record that is no entry goes to %q", got)
	}
	d.Facade = "https://facade.example.com"
	if got, want := providers(true), []string{"https://facade.example.com", "https://b.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("with a facade an entry goes to %q, want %q", got, want)
	}
}
