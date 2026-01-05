# Simple example

Build, push, and pull a single Python file.

Prefect is already installed in the base image, so we won't install it in this example.

**Note**: `ttl.sh` will delete the image after 8 hours (as tagged). If you want to keep the image longer, adjust the tag accordingly. See [ttl.sh](https://ttl.sh/) for more details.

## Usage

```bash
prefect deploy
```