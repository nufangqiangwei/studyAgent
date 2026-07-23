package main

import (
	"context"
	"errors"
	"time"
)

const (
	apiVersion             = 1
	userIDHeader           = "X-User-ID"
	maxInlineTaskInputSize = 16 << 10
)

var (
	ErrRuntimeUnavailable = errors.New("runtime is not connected")
	ErrTaskNotFound       = errors.New("task was not found")
	ErrTaskConflict       = errors.New("task conflicts with an existing task")
)

// RuntimePort is the Web entry's boundary to the Runtime-facing interaction
// adapter. HTTP handlers only depend on this projection and never receive
// Runtime stores, hosts, or business Service objects.
type RuntimePort interface {
	CreateTask(context.Context, Actor, CreateTaskInput) (TaskView, error)
	GetTask(context.Context, Actor, string) (TaskView, error)
	SubscribeApprovalRequests(context.Context, Actor) (<-chan ApprovalRequest, error)
}

type Actor struct {
	UserID string
}

type CreateTaskInput struct {
	TaskID string `json:"task_id,omitempty"`
	GoalID string `json:"goal_id,omitempty"`
	Title  string `json:"title,omitempty"`
	Input  string `json:"input"`
}

type TaskView struct {
	TaskID      string     `json:"task_id"`
	GoalID      string     `json:"goal_id,omitempty"`
	UserID      string     `json:"user_id"`
	Title       string     `json:"title,omitempty"`
	Input       string     `json:"input,omitempty"`
	Phase       string     `json:"phase"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type ApprovalRequest struct {
	ApprovalID        string     `json:"approval_id"`
	CallID            string     `json:"call_id"`
	UserID            string     `json:"user_id"`
	CapabilityRef     string     `json:"capability_ref"`
	CapabilityVersion string     `json:"capability_version"`
	RiskSummary       string     `json:"risk_summary"`
	ArgumentsDigest   string     `json:"arguments_digest"`
	RequestedAt       time.Time  `json:"requested_at"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
}

type unavailableRuntimePort struct{}

func (unavailableRuntimePort) CreateTask(context.Context, Actor, CreateTaskInput) (TaskView, error) {
	return TaskView{}, ErrRuntimeUnavailable
}

func (unavailableRuntimePort) GetTask(context.Context, Actor, string) (TaskView, error) {
	return TaskView{}, ErrRuntimeUnavailable
}

func (unavailableRuntimePort) SubscribeApprovalRequests(context.Context, Actor) (<-chan ApprovalRequest, error) {
	return nil, ErrRuntimeUnavailable
}
