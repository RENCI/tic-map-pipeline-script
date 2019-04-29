#!/bin/bash
psql -U "$POSTGRES_USER" $POSTGRES_DB < schema.sql
