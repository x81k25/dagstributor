INSERT INTO atp.training (
    imdb_id,
    tmdb_id,
    label,
    media_type,
    media_title,
    season,
    episode,
    release_year,
    budget,
    revenue,
    runtime,
    origin_country,
    production_companies,
    production_countries,
    production_status,
    original_language,
    spoken_languages,
    genre,
    original_media_title,
    tagline,
    overview,
    tmdb_rating,
    tmdb_votes,
    rt_score,
    metascore,
    imdb_rating,
    imdb_votes,
    created_at,
    updated_at
)
WITH grouped_media AS (
    SELECT
        imdb_id,
        ARRAY_AGG(rejection_status) AS rejection_statuses,
        -- Take first non-null value for each field
        MAX(tmdb_id) AS tmdb_id,
        MAX(media_type) AS media_type,
        MAX(media_title) AS media_title,
        MAX(season) AS season,
        MAX(episode) AS episode,
        MAX(release_year) AS release_year,
        -- New metadata columns
        MAX(budget) AS budget,
        MAX(revenue) AS revenue,
        MAX(runtime) AS runtime,
        MAX(origin_country) AS origin_country,
        MAX(production_companies) AS production_companies,
        MAX(production_countries) AS production_countries,
        MAX(production_status) AS production_status,
        MAX(original_language) AS original_language,
        MAX(spoken_languages) AS spoken_languages,
        MAX(genre) AS genre,
        MAX(original_media_title) AS original_media_title,
        MAX(tagline) AS tagline,
        MAX(overview) AS overview,
        MAX(tmdb_rating) AS tmdb_rating,
        MAX(tmdb_votes) AS tmdb_votes,
        MAX(rt_score) AS rt_score,
        MAX(metascore) AS metascore,
        MAX(imdb_rating) AS imdb_rating,
        MAX(imdb_votes) AS imdb_votes,
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS created_at,
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS updated_at
    FROM atp.media
    WHERE imdb_id IS NOT NULL
    GROUP BY imdb_id
),
processed_media AS (
    SELECT
        *,
        -- Check if any rejection_status is 'accepted' or 'override'
        CASE
            WHEN 'accepted' = ANY(rejection_statuses) OR 'override' = ANY(rejection_statuses) THEN 'would_watch'::atp.label_type
            WHEN 'unfiltered' = ALL(rejection_statuses) THEN NULL
            ELSE 'would_not_watch'::atp.label_type
        END AS label
    FROM grouped_media
)
SELECT
    pm.imdb_id,
    pm.tmdb_id,
    pm.label,
    pm.media_type,
    COALESCE(pm.media_title, '') AS media_title,
    pm.season,
    pm.episode,
    pm.release_year,
    pm.budget,
    pm.revenue,
    pm.runtime,
    pm.origin_country,
    pm.production_companies,
    pm.production_countries,
    pm.production_status,
    pm.original_language,
    pm.spoken_languages,
    pm.genre,
    pm.original_media_title,
    pm.tagline,
    pm.overview,
    pm.tmdb_rating,
    pm.tmdb_votes,
    pm.rt_score,
    pm.metascore,
    pm.imdb_rating,
    pm.imdb_votes,
    pm.created_at,
    pm.updated_at
FROM processed_media pm
LEFT JOIN atp.training t ON pm.imdb_id = t.imdb_id
WHERE pm.label IS NOT NULL
  AND t.imdb_id IS NULL;