package capability

import (
	"fmt"
	"sort"
	"strings"
)

type catalogEntry struct {
	descriptor CapabilityDescriptor
	provider   CapabilityProvider
}

// Catalog is immutable after construction. Descriptor and provider identity
// are therefore fixed for the Plan revisions that reference this module.
type Catalog struct {
	entries map[string]catalogEntry
}

func NewCatalog(providers []CapabilityProvider) (*Catalog, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("at least one capability provider is required")
	}
	entries := make(map[string]catalogEntry)
	providerRefs := make(map[string]struct{})
	for _, provider := range providers {
		if provider == nil {
			return nil, fmt.Errorf("capability provider is nil")
		}
		providerRef := clean(provider.Ref())
		if providerRef == "" {
			return nil, fmt.Errorf("capability provider ref is required")
		}
		if _, exists := providerRefs[providerRef]; exists {
			return nil, fmt.Errorf("capability provider %q is duplicated", providerRef)
		}
		providerRefs[providerRef] = struct{}{}
		descriptors := provider.Describe()
		if len(descriptors) == 0 {
			return nil, fmt.Errorf("capability provider %q has no descriptors", providerRef)
		}
		for _, descriptor := range descriptors {
			descriptor = descriptor.Clone()
			descriptor.Ref, descriptor.Version = clean(descriptor.Ref), clean(descriptor.Version)
			descriptor.ProviderRef, descriptor.DescriptorRevision = clean(descriptor.ProviderRef), clean(descriptor.DescriptorRevision)
			if descriptor.Ref == "" || descriptor.Version == "" || descriptor.DescriptorRevision == "" {
				return nil, fmt.Errorf("capability descriptor ref, version, and revision are required")
			}
			if descriptor.ProviderRef != providerRef {
				return nil, fmt.Errorf("capability %q provider %q does not match %q", descriptor.Ref, descriptor.ProviderRef, providerRef)
			}
			if !descriptor.ExecutionKind.Valid() {
				return nil, fmt.Errorf("capability %q execution kind %q is invalid", descriptor.Ref, descriptor.ExecutionKind)
			}
			if descriptor.ExecutionKind == ExecutionEffect && (clean(descriptor.ExecutorRef) == "" || descriptor.EffectType == "") {
				return nil, fmt.Errorf("effect capability %q requires executor ref and effect type", descriptor.Ref)
			}
			if descriptor.ExecutionKind == ExecutionServiceCommand && (descriptor.ExecutorRef != "" || descriptor.EffectType != "") {
				return nil, fmt.Errorf("service-command capability %q cannot declare an effect executor", descriptor.Ref)
			}
			if descriptor.ExecutionKind == ExecutionServiceCommand && (descriptor.CommandType == "" || descriptor.CommandVersion <= 0 || descriptor.ReplyType == "" || descriptor.ReplyVersion <= 0) {
				return nil, fmt.Errorf("service-command capability %q requires command and reply contracts", descriptor.Ref)
			}
			if descriptor.ExecutionKind == ExecutionEffect && (descriptor.CommandType != "" || descriptor.CommandVersion != 0 || descriptor.ReplyType != "" || descriptor.ReplyVersion != 0) {
				return nil, fmt.Errorf("effect capability %q cannot declare service message contracts", descriptor.Ref)
			}
			key := descriptorKey(descriptor.Ref, descriptor.Version)
			if _, exists := entries[key]; exists {
				return nil, fmt.Errorf("capability descriptor %q is duplicated", key)
			}
			entries[key] = catalogEntry{descriptor: descriptor, provider: provider}
		}
	}
	return &Catalog{entries: entries}, nil
}

func (c *Catalog) Resolve(ref, version string) (CapabilityDescriptor, CapabilityProvider, bool) {
	if c == nil {
		return CapabilityDescriptor{}, nil, false
	}
	entry, found := c.entries[descriptorKey(clean(ref), clean(version))]
	return entry.descriptor.Clone(), entry.provider, found
}

func (c *Catalog) Descriptors() []CapabilityDescriptor {
	if c == nil {
		return nil
	}
	keys := make([]string, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]CapabilityDescriptor, 0, len(keys))
	for _, key := range keys {
		values = append(values, c.entries[key].descriptor.Clone())
	}
	return values
}

func descriptorKey(ref, version string) string {
	return strings.TrimSpace(ref) + "@" + strings.TrimSpace(version)
}
