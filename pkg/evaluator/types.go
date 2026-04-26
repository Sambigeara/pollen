// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import "slices"

// JSON field names follow the conventional authorisation-request shape
// (subject/action/resource/context), so a PDP seed receives
// json.Marshal(Request) as-is.

type Subject struct {
	Properties map[string]any `json:"properties,omitempty"`
	Type       string         `json:"type,omitempty"`
	ID         string         `json:"id,omitempty"`
}

type Action struct {
	Properties map[string]any `json:"properties,omitempty"`
	Name       string         `json:"name"`
}

type Resource struct {
	Properties map[string]any `json:"properties,omitempty"`
	Type       ResourceType   `json:"type,omitempty"`
	ID         string         `json:"id,omitempty"`
}

type Request struct {
	Context  map[string]any `json:"context,omitempty"`
	Subject  Subject        `json:"subject"`
	Resource Resource       `json:"resource"`
	Action   Action         `json:"action"`
}

// Decision=true means allow. Context carries a reason_user string the
// dispatch site surfaces to the denied caller.
type Decision struct {
	Context  map[string]any `json:"context,omitempty"`
	Decision bool           `json:"decision"`
}

// ResourceType is closed so unknown values fail validation at config
// load.
type ResourceType string

const ResourceService ResourceType = "service"

func NewResource(rt ResourceType, id string, props map[string]any) Resource {
	return Resource{Type: rt, ID: id, Properties: props}
}

// GateName is closed so config references are validated at load time;
// unknown names fail daemon startup rather than silently falling through.
type GateName string

const GateServiceConnect GateName = "service_connect"

var allGateNames = []GateName{GateServiceConnect}

func AllGateNames() []GateName { return slices.Clone(allGateNames) }

func (g GateName) Valid() bool { return slices.Contains(allGateNames, g) }
