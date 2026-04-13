# arbk-gh-probe

Probe to test whether GitHub Actions runners (Azure IPs) can reach `arbk.rks-gov.net`,
solve a Cloudflare Turnstile token, and successfully use that token to call the ARBK API.

If this works, GitHub Actions can host a distributed solver farm for the main scraper.
If not, we abandon the approach.

## What the workflow does

1. Prints the runner's public IP
2. Hits `arbk.rks-gov.net/api/api/Home/GetDate` (no auth, should always work)
3. Installs xvfb + clones `cf-clearance-scraper`
4. Starts the scraper under xvfb in the background
5. Solves ONE Turnstile token
6. Uses that token to call `TeDhenatBiznesit?nRegjistriId=1`
7. Reports HTTP status and response

## Trigger it

Via GitHub CLI:
```
gh workflow run probe.yml
```

Or via the Actions tab in the GitHub UI.
