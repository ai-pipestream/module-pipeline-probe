# Module Pipeline Probe

Live testing and observation service for pipeline data streams.

## Build Profiles

This module supports build profiles to switch between internal (Gitea) and external (GitHub) build contexts:

### Usage

**Environment Variable (Recommended):**
```bash
# Internal builds (Gitea context)
export GRADLE_BUILD_PROFILE=internal
./gradlew build

# External builds (GitHub context)
export GRADLE_BUILD_PROFILE=external
./gradlew build
```

**Command Line Property:**
```bash
# Alternative: use -P flag
./gradlew build -Pbuild.profile=internal
```

### Profile Behavior

- **internal**: Uses internal repositories (Reposilite, Gitea Maven)
- **external** (default): Uses public repositories (Maven Central, Spring repos)

### Environment Variables

The following environment variables are used for repository authentication:

- `REPOS_USER` / `REPOS_PAT`: Reposilite credentials
- `GIT_USER` / `GIT_PAT`: Gitea Maven credentials

## Development

### Testing

The module uses WireMock for gRPC service mocking in tests. Run tests with:

```bash
./gradlew test
```

### Running

```bash
./gradlew quarkusDev
```

## Architecture

This module provides gRPC services for testing and observing pipeline data streams, with proper mocking for dependent services.