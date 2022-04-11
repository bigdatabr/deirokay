# Contributing to Deirokay

It's amazing that you are willing to contribute to Deirokay!
Everyone knows that community is one of the things that makes
a great tool to be great.

By following these guidelines, we hope you will make your contributions
as useful and effective as possible, being more likely to be easily
accepted and quickly made available in future releases.


## Ground rules

1. Fork this project, start developing from `next` branch and always
propose Pull Requests for this branch.

2. Make sure you install and use `pre-commit` to sanitize all your commits
according to our project's styles:

```bash
pip install pre-commit
pre-commit install
```

3. Create an env based on the `dev` requirements.

```bash
pip install -e .[dev]
```

4. Follow [conventionalcommits](https://www.conventionalcommits.org/en/v1.0.0/)
specification to write your commits.

5. Pull Requests for new features require that you:
  - Write docstrings for all public methods and update the `docs` when
  needed;
  - Write Pytest unit tests with good coverage for all
  your features and improvements. Existing unit tests should not fail
  after your contribution.


## Your First Contribution

- Review a [Pull Request](https://github.com/bigdatabr/deirokay/pulls);
- Update the [documentation](https://github.com/bigdatabr/deirokay/tree/master/docs/source);
- Fix an [Issue](https://github.com/bigdatabr/deirokay/issues);
- Write a tutorial.
