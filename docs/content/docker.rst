Docker images
********************

This project comes with number of docker images associated with it.
Here is a short explanation of all of them.

noiz
=================

Those are images build on the base of main Dockerfile of the project.
Built every master merge and every time the Dockerfile is modified.

noiz:stable
+++++++++++++++++

Since current development of Noiz does not include any path of stabilizing branches periodic releases of the software,
this branch is not stable in the same way as you could see it usually.
By calling it stable, I left space for possibility of moving into different style of development.

Currently, image tagged as `noiz:stable` is an image build on the base of `master` branch.

noiz:latest
+++++++++++++++++

In case branch modifies Dockerfile, it is build on branch.
Otherwise it is built every commit to master and is equivalent to `noiz:stable`


unittesting
=================

Image that is based on simple Python 3.8 image and contains all requirements of noiz.
It is being used to speed up CI runs by skipping the downloading and installation of all of the requirements for noiz
in jobs such as unit testing, linting, documentation building.
It is built every commit to master.
Based on `python:3.8-slim-buster` image.

postgres
=================

This is image of PostgreSQL database that is being used within the noiz deployments.
The only difference compared to the original image is that this one contains initiation SQL scripts.
Those scripts contains some required commands that have to be run before using that DB with noiz.
This means that the Postgres is possible to be set up automatically, without additional actions done by you.
It is built every commit to master.
Based on `postgres:13` image.
