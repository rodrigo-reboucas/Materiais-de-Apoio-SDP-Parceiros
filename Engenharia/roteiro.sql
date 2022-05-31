-- trata_movies
create table hive.trusted.movies with (format = 'parquet') as
select
  try_cast(movieid as integer) movieid,
  title,
  try_cast(
    regexp_extract(
      trim(title),
      '\(([0-9]*)\)$',
      1
    ) as integer
  ) year,
  genre
from
  hive.raw.table_movies
  CROSS JOIN UNNEST(split(genres, '|')) AS t (genre)
where
  dt = (
    select
      max(dt)
    from
      hive.raw."table_movies$partitions"
  ) ---------------------------------------------------------------
  -- trata_ratings
  create table hive.trusted.ratings with (format = 'parquet') as
select
  cast(movieid as integer) movieid,
  cast(userid as integer) userid,
  cast(rating as decimal) rating,
  cast(
    from_unixtime(cast(timestamp as double)) as timestamp
  ) "timestamp"
from
  hive.raw.table_ratings
where
  dt = (
    select
      max(dt)
    from
      hive.raw."table_ratings$partitions"
  ) ---------------------------------------------------------------
  -- join_imdb_tables
  create table hive.service.imdb with (format = 'parquet') as (
    with ratings as (
      select
        movieid,
        avg(try_cast(rating as DOUBLE)) rating
      from
        hive.trusted.ratings
      group by
        movieid
    )
    select
      movies.movieid,
      movies.title,
      movies.year,
      movies.genre,
      ratings.rating
    from
      hive.trusted.movies
      inner join ratings on movies.movieid = ratings.movieid
  ) ---------------------------------------------------------------