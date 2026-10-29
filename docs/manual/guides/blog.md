---
title: A blog and RSS feed
doc_id: guide:blog
---

# A blog and RSS feed

Build a host-only blog from shared Markdown fixtures. Bash and a compatible
`pond` are required; there is no pond initialization, service, or credential.
Legacy page layouts use matching local vendor assets from the binary package.

{{ source path="/testsuite/examples/blog-rss/site.yaml" language="yaml" /}}

{{ source path="/testsuite/examples/blog-rss/posts/first.md" language="markdown" /}}

{{ source path="/testsuite/examples/blog-rss/run.sh" language="bash" /}}

Run `bash testsuite/examples/blog-rss/run.sh` from a checkout or unpack the
[complete package](/downloads/blog-rss.tar). `EXAMPLE_OUTPUT` must be absent
when specified; otherwise a fresh temporary directory is created.

Test `214` asserts two feed entries, newest-first order, absolute links under
`https://example.invalid/sample-blog/`, and hidden/undated exclusions.
The `.invalid` origin is a placeholder for generated links, never contacted.
Hidden pages still render: `hidden: true` is not access control.

Set `site.site_url` to your real origin and `site.base_url` to the actual
hosting prefix before publishing. Without a site URL no feed is generated.
See [frontmatter](../reference/sitegen/frontmatter.md) and
[feed configuration](../reference/sitegen/configuration.md#feeds).
