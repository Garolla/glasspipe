-- Database layout. `raw` holds both lanes (raw_nrt is SQLMesh-owned DDL,
-- raw_batch is owned here because batch_extract writes to it directly --
-- see ARCHITECTURE.md, principle 5). `marts` holds SQLMesh's output.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS marts;
