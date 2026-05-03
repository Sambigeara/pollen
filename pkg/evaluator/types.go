// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import "slices"

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

type Decision struct {
	Context  map[string]any `json:"context,omitempty"`
	Decision bool           `json:"decision"`
}

type ResourceType string

const ResourceService ResourceType = "service"

func NewResource(rt ResourceType, id string, props map[string]any) Resource {
	return Resource{Type: rt, ID: id, Properties: props}
}

type GateName string

const GateServiceConnect GateName = "service_connect"

var allGateNames = []GateName{GateServiceConnect}

func AllGateNames() []GateName { return slices.Clone(allGateNames) }

func (g GateName) Valid() bool { return slices.Contains(allGateNames, g) }
