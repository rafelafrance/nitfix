DROP TABLE IF EXISTS images;
CREATE TABLE images (
  id            TEXT PRIMARY KEY,
  file_name     TEXT UNIQUE,
  image_created TEXT
);
