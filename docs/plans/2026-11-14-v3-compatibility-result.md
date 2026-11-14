# Vfs v3 compatibility result

Starting branch: `main`. Starting revision: `eba18f591b66a88efab4aacfd7cd50ab2475efee`.
The primary branch was retained to preserve local and concurrent work.
The delivery report names the exact published branch and commit.

Status: checked v3 source candidate.

## Boundary and implementation

VFS owns adapter operations, captures, and revisions. Hosts own Action
wrappers and policy. VFS has no core runtime dependency. The existing v3 Action
guide is retained. The new `integration/jido_v3_contract.exs` compiles that
exact guide example and runs it through consolidated core. The package file
list includes this source contract. The README names the correct core owner.

## Checks

- `mix test`: 494 passed, including 2 doctests; 557 excluded.
- `mix quality`: passed, including Dialyzer and Doctor.
- From Workspace: `MIX_ENV=test mix run /Users/mhostetler/Source/Jido/jido_workspace/jido_vfs/integration/jido_v3_contract.exs`: 1 passed. The test reads a temporary file and checks invalid Action input.
- Workspace's 27 v3 boundary tests passed with the local VFS and Shell sources.
- Isolated Jidoka: 4 focused tests passed; see shared evidence.
- `mix hex.build`: passed.

The standard VFS helper did not find Minio, so S3 tests were excluded. Standard
integration and flaky exclusions also applied. Local Git adapter tests ran.
S3, remote GitHub writes, live storage services, and unselected integration
suites are not verified by this result.

## Shared evidence and limits

Date: 2026-11-14. GitHub API checks confirmed consolidated core `release/v3`
at `047bf746330a2fbe7c3b25c045552e0303c97eaf`. Tests used Elixir 1.20.4,
OTP 29.1.2. Core owns Action, Signal, and AI modules. No separate legacy
core package is present in either new consumer fixture dependency tree.
No mandatory core runtime dependency was added to a standalone package.

The package sources and consumer fixtures were self-checked. A separate source
review checked the Harness reconciliation. No live model or paid provider
check ran. No live app home, checkpoint, or database was changed.

Jidoka started on `release/v3` at
`5b697d3d3a0f96bc7baf063687d4a98f31a0565a`, with local checkpoint work.
Its source was copied to `/private/tmp/five-v3/jidoka`. Only that copy's
manifest used paths to the four candidate libraries. Core remained pinned to
`047bf746`. The command below passed 4 tests with a temporary SQLite database:

```sh
MIX_TARGET=desktop ERL_FLAGS='+S 4:4' mix test test/jidoka/workspaces_test.exs test/jidoka/inference/detector_test.exs
```

The first two tests cover write/read/list/capture, mutation policy, storage
selection refusal, and close. The other two cover provider discovery and
failure normalization. They do not run a live Harness provider. The original
Jidoka checkout and its existing local changes were preserved.

## Release order

Publish the required consolidated core and shared dependencies first. ReqLLM
1.30.0 and Zoi 0.19.0 are already available. Release VFS before Shell, then
Workspace, then update and check Jidoka dependency pins. Character and Harness
can release independently after their shared dependencies are available.
Migrate Hancho before claiming it supports the new Harness candidate. No
registry publication, release, merge, or PR was part of this work.

## Static config audit

From the primary source directory, `mix credo --strict --config-file .credo.exs`
passed with 18 checks. This explicitly selects the repository config and
avoids temporary-directory config ancestry. The shared `/private/tmp/.credo.exs`
was not changed.
