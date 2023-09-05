#!/usr/bin/sh
cd $1
npm install
npm audit fix --force
