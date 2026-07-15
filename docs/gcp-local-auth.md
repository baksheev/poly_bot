# Repository-local GCP authentication

This repository uses the same account-selection approach as `marketing-dwh`.
The wrapper below points Google Cloud SDK at an ignored, repository-local
configuration directory:

```bash
./scripts/gcloud-local
```

It sets `CLOUDSDK_CONFIG` to `.gcloud/`. The selected account, project,
credentials, and Application Default Credentials therefore remain isolated
from global `gcloud` state and other repositories.

## First-time setup

Authenticate with the Google account intended for this project:

```bash
./scripts/gcloud-local auth login --no-launch-browser
```

Select the project:

```bash
./scripts/gcloud-local config set project poly-bot-502515
```

Verify both values:

```bash
./scripts/gcloud-local auth list
./scripts/gcloud-local config list
```

For libraries or scripts that use Application Default Credentials locally,
initialize repository-local ADC separately:

```bash
./scripts/gcloud-local auth application-default login --no-launch-browser
./scripts/gcloud-local auth application-default set-quota-project poly-bot-502515
```

## Switching accounts

Add another account if necessary:

```bash
./scripts/gcloud-local auth login --no-launch-browser
```

Select an already authenticated account:

```bash
./scripts/gcloud-local config set account YOUR_ACCOUNT@example.com
./scripts/gcloud-local auth list
```

Use `./scripts/gcloud-local` for all local GCP commands in this repository.
Production Compute Engine uses an attached service account and metadata-server
tokens; it must not depend on this local directory or a long-lived JSON key.
