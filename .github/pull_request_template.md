## Description
<!-- What does this PR do? Link to related issue: Fixes #__ -->

## Type of Change
- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to change)
- [ ] 📝 Documentation update
- [ ] 🔧 Refactor / chore

## Checklist
- [ ] Tests pass: `make check` (~15s)
- [ ] Linter passes: `golangci-lint run`
- [ ] Documentation updated (if applicable)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] No new exported types added to root package without updating `STABLE_API.md`

## Performance Impact
<!-- If this touches a hot path, include benchmark results:
     go test -bench=BenchmarkComparative -benchmem -count=3
     Otherwise, delete this section. -->
