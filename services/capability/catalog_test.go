package capability

import "testing"

func TestCatalogAllowsNoCapabilityProviders(t *testing.T) {
	catalog, err := NewCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	if values := catalog.Descriptors(); len(values) != 0 {
		t.Fatalf("descriptors=%#v", values)
	}
	if _, _, found := catalog.Resolve("missing", "v1"); found {
		t.Fatal("empty catalog resolved a capability")
	}
}
