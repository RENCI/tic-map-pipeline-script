#!/bin/bash
set -e

echo schema
psql -U "$POSTGRES_USER" $POSTGRES_DB < schema.sql

echo exit
