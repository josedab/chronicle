# Community Setup Guide

How to set up and manage the Chronicle community channels.

## Discord Server Structure

### Server Name
Chronicle TSDB

### Channels

| Channel | Purpose | Who Posts |
|---------|---------|-----------|
| `#announcements` | Releases, breaking changes, events | Maintainers only |
| `#general` | General discussion | Everyone |
| `#help` | Questions and troubleshooting | Everyone |
| `#development` | Contributing, PRs, code discussion | Contributors |
| `#showcase` | Share what you built with Chronicle | Everyone |
| `#proposals` | Feature proposals and RFC discussion | Everyone |
| `#benchmarks` | Share benchmark results on different hardware | Everyone |
| `#edge-iot` | Edge/IoT specific discussion | Everyone |
| `#off-topic` | Non-Chronicle discussion | Everyone |

### Roles

| Role | Color | Permissions |
|------|-------|-------------|
| `@maintainer` | Red | Admin, manage channels, pin messages |
| `@contributor` | Green | Assigned after first merged PR |
| `@community` | Blue | Default role for all members |

### Welcome Message

```
Welcome to the Chronicle TSDB community! 👋

Chronicle is an embedded time-series database for Go.

🔗 **Links:**
- GitHub: https://github.com/chronicle-db/chronicle
- Docs: https://chronicle-db.github.io/chronicle
- Playground: https://chronicle-db.github.io/chronicle/playground

📋 **Getting Started:**
1. Read the README for a quick overview
2. Try the 5-minute demo: docs/DEMO_5_MINUTES.md
3. Check #help if you have questions
4. Share what you build in #showcase!

🤝 **Want to contribute?**
- Check docs/BOUNTY_PROGRAM.md for paid opportunities
- Look for `good first issue` labels on GitHub
- Join #development to discuss

Please follow our Code of Conduct: CODE_OF_CONDUCT.md
```

### Moderation Rules

1. Be respectful and constructive
2. No spam or self-promotion (unless in #showcase)
3. Keep discussions on-topic in specialized channels
4. Use threads for long conversations
5. Don't DM maintainers for support — use #help

## GitHub Discussions

Enable GitHub Discussions with these categories:

| Category | Description | Format |
|----------|-------------|--------|
| **Announcements** | Official project updates | Announcement |
| **Q&A** | Questions and answers | Q&A |
| **Ideas** | Feature requests and proposals | Open |
| **Show and Tell** | Share what you built | Open |
| **General** | Everything else | Open |

## Communication Cadence

| Activity | Frequency | Channel |
|----------|-----------|---------|
| Release announcement | Per release | Discord #announcements, GitHub Release |
| Weekly update | Weekly | Discord #general, GitHub Discussion |
| Contributor spotlight | Monthly | Discord #announcements |
| Community call | Monthly (optional) | Discord voice channel |
| Roadmap review | Quarterly | GitHub Discussion |

## Metrics to Track

- Discord member count (target: 50 in 3 months)
- Daily active users (target: 5+)
- GitHub stars (target: 100 in 6 months)
- Monthly PRs from non-maintainers (target: 3+)
- GitHub Discussion engagement (target: 10+ threads/month)
