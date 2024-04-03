#!/bin/bash

wasm-pack build --scope=cswinter
cd pkg
npm publish --access-public
