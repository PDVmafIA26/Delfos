Executing the project (reporting and messaging guilds, notifications container):
`docker compose up -d`

To try out the notify endpoint, simply use the fastapi integrated docs, by running the previous command followed by opening a browser and going to:
`localhost:8000/docs`

Prerrequisites:

- Docker daemon running in the background.
- `.env` file in the same folder as this README.md, with the content hinted in the provided `.env.example`.
