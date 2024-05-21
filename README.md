# follow-the-crypto-backend

## Quick Start

1. Create a project using the [Firebase console](https://console.firebase.google.com/) (or use an existing Google Cloud project with billing and the Cloud Logging APIs enabled).
2. Follow the instructions
   to [Initialize Cloud Firestore](https://firebase.google.com/docs/firestore/quickstart#python_1) (see the "Initialize
   on your own server" section) and save your service account credentials in a file named `service.json`.
3. Sign up for an [FEC API key](https://api.open.fec.gov/developers/) if you don't already have one.
4. In your `.env` file, specify a `FEC_API_KEY` environment variable that contains the FEC API key.
5. Create a Python virtual environment with [Poetry](https://python-poetry.org/docs/#installation) installed.
6. Run `poetry install` to install the project dependencies.
