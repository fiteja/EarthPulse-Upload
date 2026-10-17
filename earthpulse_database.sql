-- =====================================================================
-- Fitaje EarthPulse - Base de dados unica (PostgreSQL 16 + PostGIS 3.4)
-- NASA Space Apps Challenge 2026 | Angola
--
-- Junta o Esquema de Backend (dados, GIS, pipeline, API) com o Plano de
-- Implantacao (releases, ingestao, validacao Go/No-Go, backups, papeis).
--
-- Uso:
--   createdb -U earthpulse earthpulse
--   psql -U earthpulse -d earthpulse -f earthpulse_database.sql
--
-- Nota: em producao o esquema deve evoluir por migracoes Alembic. Este
-- ficheiro serve de baseline (revisao inicial) e de referencia unica.
-- Nenhum segredo neste ficheiro: as palavras-passe dos papeis definem-se
-- a parte (ver seccao 9).
-- =====================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------
-- 1. Catalogo: indicadores e datasets
-- ---------------------------------------------------------------------

CREATE TABLE indicators (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(30)  UNIQUE NOT NULL,          -- ex.: ndvi, population
    name        TEXT         NOT NULL,
    unit        TEXT         NOT NULL,                 -- exigido pela legenda/painel
    source      TEXT         NOT NULL,                 -- ex.: NASA Earthdata
    description TEXT,
    valid_min   DOUBLE PRECISION,                      -- intervalo valido (NULL = sem limite)
    valid_max   DOUBLE PRECISION,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (valid_min IS NULL OR valid_max IS NULL OR valid_min <= valid_max)
);

-- Rastreabilidade de cada ficheiro de origem (GeoTIFF/COG)
CREATE TABLE datasets (
    id              SERIAL PRIMARY KEY,
    name            TEXT        NOT NULL,
    source          TEXT        NOT NULL,              -- ex.: MODIS, HLS, GPW (SEDAC)
    product_version TEXT,
    license         TEXT,
    crs             TEXT        NOT NULL DEFAULT 'EPSG:4326',
    resolution      TEXT,                              -- ex.: 250 m
    nodata_value    DOUBLE PRECISION,
    file_path       TEXT        NOT NULL,              -- data/raw/... ou data/processed/...
    checksum_sha256 TEXT,
    acquired_at     DATE        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, file_path)
);

-- ---------------------------------------------------------------------
-- 2. Geografia: provincias
-- ---------------------------------------------------------------------

CREATE TABLE provinces (
    id   SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name TEXT        NOT NULL,
    geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX idx_provinces_geom ON provinces USING GIST (geom);

-- ---------------------------------------------------------------------
-- 3. Observacoes e estatisticas
-- ---------------------------------------------------------------------

-- Valores em falta ficam NULL (nunca zero). NaN nao e permitido.
CREATE TABLE observations (
    id           BIGSERIAL PRIMARY KEY,
    indicator_id INT  NOT NULL REFERENCES indicators(id),
    dataset_id   INT  NOT NULL REFERENCES datasets(id),
    province_id  INT  REFERENCES provinces(id),
    observed_at  DATE NOT NULL,
    value        DOUBLE PRECISION,
    geom         geometry(Point, 4326) NOT NULL,
    CONSTRAINT ck_obs_not_nan CHECK (value IS NULL OR value <> 'NaN'::float8)
);
CREATE INDEX idx_obs_geom       ON observations USING GIST (geom);
CREATE INDEX idx_obs_ind_date   ON observations (indicator_id, observed_at);
CREATE INDEX idx_obs_province   ON observations (province_id, indicator_id, observed_at);
CREATE INDEX idx_obs_dataset    ON observations (dataset_id);
-- Pipeline idempotente: reexecutar nao duplica celulas
CREATE UNIQUE INDEX uq_obs_cell ON observations
    (indicator_id, dataset_id, observed_at, ST_GeoHash(geom, 12));

-- Estatisticas pre-calculadas por provincia e periodo (painel rapido)
CREATE TABLE province_stats (
    province_id  INT  NOT NULL REFERENCES provinces(id),
    indicator_id INT  NOT NULL REFERENCES indicators(id),
    period_start DATE NOT NULL,
    period_end   DATE NOT NULL,
    mean         DOUBLE PRECISION,
    min          DOUBLE PRECISION,
    max          DOUBLE PRECISION,
    count        INT NOT NULL DEFAULT 0 CHECK (count >= 0),
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (province_id, indicator_id, period_start, period_end),
    CHECK (period_start <= period_end)
);
CREATE INDEX idx_pstats_ind_period ON province_stats (indicator_id, period_start, period_end);

-- Validacao automatica do intervalo valido de cada indicador
-- (NDVI entre -1 e 1; populacao nao negativa)
CREATE OR REPLACE FUNCTION trg_check_observation_range() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    vmin DOUBLE PRECISION;
    vmax DOUBLE PRECISION;
BEGIN
    SELECT valid_min, valid_max INTO vmin, vmax
    FROM indicators WHERE id = NEW.indicator_id;

    IF NEW.value IS NOT NULL AND (
         (vmin IS NOT NULL AND NEW.value < vmin) OR
         (vmax IS NOT NULL AND NEW.value > vmax)) THEN
        RAISE EXCEPTION 'valor % fora do intervalo valido [%, %] para o indicador %',
            NEW.value, vmin, vmax, NEW.indicator_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER trg_observations_range
    BEFORE INSERT OR UPDATE OF value, indicator_id ON observations
    FOR EACH ROW EXECUTE FUNCTION trg_check_observation_range();

-- ---------------------------------------------------------------------
-- 4. Operacao: ingestao, qualidade dos dados, releases, backups
-- ---------------------------------------------------------------------

-- Cada execucao da ingestao/pipeline (a API nao depende da NASA em tempo real)
CREATE TABLE ingestion_runs (
    id           BIGSERIAL PRIMARY KEY,
    dataset_id   INT REFERENCES datasets(id),
    stage        TEXT NOT NULL DEFAULT 'full'
                 CHECK (stage IN ('download','clean','reproject','indicators','aggregate','load','full')),
    status       TEXT NOT NULL DEFAULT 'running'
                 CHECK (status IN ('running','success','failed')),
    rows_loaded  BIGINT NOT NULL DEFAULT 0,
    error_code   TEXT,                                  -- ex.: upstream_unavailable
    message      TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
CREATE INDEX idx_runs_status ON ingestion_runs (status, started_at DESC);

-- Verificacoes de qualidade (criterios de aceitacao "Dados" do Go/No-Go)
CREATE TABLE data_quality_checks (
    id         BIGSERIAL PRIMARY KEY,
    run_id     BIGINT REFERENCES ingestion_runs(id) ON DELETE SET NULL,
    check_name TEXT    NOT NULL,                        -- ex.: ndvi_range, population_non_negative
    passed     BOOLEAN NOT NULL,
    details    JSONB   NOT NULL DEFAULT '{}'::jsonb,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_dqc_name ON data_quality_checks (check_name, checked_at DESC);

-- Registo das publicacoes por ambiente (tags vX.Y.Z, rollback, migracoes)
CREATE TABLE releases (
    id                 SERIAL PRIMARY KEY,
    environment        TEXT NOT NULL CHECK (environment IN ('local','staging','production')),
    version_tag        TEXT NOT NULL,                   -- ex.: v1.0.0
    git_sha            TEXT,
    migration_revision TEXT,                            -- revisao Alembic aplicada
    status             TEXT NOT NULL DEFAULT 'deployed'
                       CHECK (status IN ('deployed','failed','rolled_back')),
    rolled_back_to     TEXT,                            -- tag reposta, se houve rollback
    deployed_by        TEXT,
    notes              TEXT,
    deployed_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_releases_env ON releases (environment, deployed_at DESC);

-- Registo de backups e testes de restauro (criterio "Operacao" do Go/No-Go)
CREATE TABLE backup_log (
    id            SERIAL PRIMARY KEY,
    kind          TEXT NOT NULL CHECK (kind IN ('daily','pre_migration','snapshot','pre_release')),
    file_name     TEXT NOT NULL,
    location      TEXT,                                 -- ex.: armazenamento fora do servidor
    size_bytes    BIGINT,
    restore_tested BOOLEAN NOT NULL DEFAULT false,
    restore_tested_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 5. Funcoes de consulta usadas pela API
-- ---------------------------------------------------------------------

-- Ponto -> provincia (atencao: longitude primeiro em ST_MakePoint)
CREATE OR REPLACE FUNCTION get_province_by_point(p_lat DOUBLE PRECISION, p_lon DOUBLE PRECISION)
RETURNS TABLE (code VARCHAR, name TEXT)
LANGUAGE sql STABLE AS $$
    SELECT p.code, p.name
    FROM provinces p
    WHERE ST_Contains(p.geom, ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326))
    LIMIT 1;
$$;

-- Serie temporal na celula mais proxima (KNN com GiST).
-- Sem linhas => 404 no_data / out_of_coverage (celula mais proxima alem de p_max_dist graus).
CREATE OR REPLACE FUNCTION get_point_series(
    p_indicator TEXT,
    p_lat       DOUBLE PRECISION,
    p_lon       DOUBLE PRECISION,
    p_from      DATE,
    p_to        DATE,
    p_max_dist  DOUBLE PRECISION DEFAULT 0.1
) RETURNS TABLE (observed_at DATE, value DOUBLE PRECISION, province_code VARCHAR)
LANGUAGE sql STABLE AS $$
    WITH ind AS (SELECT id FROM indicators WHERE code = p_indicator),
    pt AS (SELECT ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326) AS g),
    nearest AS (
        SELECT o.geom
        FROM observations o, ind, pt
        WHERE o.indicator_id = ind.id
        ORDER BY o.geom <-> pt.g
        LIMIT 1
    )
    SELECT o.observed_at, o.value, pr.code
    FROM observations o
    JOIN ind     ON o.indicator_id = ind.id
    JOIN nearest n ON o.geom = n.geom
    CROSS JOIN pt
    LEFT JOIN provinces pr ON pr.id = o.province_id
    WHERE o.observed_at BETWEEN p_from AND p_to
      AND ST_DWithin(n.geom, pt.g, p_max_dist)
    ORDER BY o.observed_at;
$$;

-- Recalculo das estatisticas por provincia (idempotente: faz upsert)
CREATE OR REPLACE FUNCTION refresh_province_stats(
    p_indicator TEXT,
    p_from      DATE,
    p_to        DATE
) RETURNS INT
LANGUAGE plpgsql AS $$
DECLARE
    n INT;
BEGIN
    INSERT INTO province_stats
        (province_id, indicator_id, period_start, period_end, mean, min, max, count, computed_at)
    SELECT o.province_id, o.indicator_id, p_from, p_to,
           avg(o.value), min(o.value), max(o.value), count(o.value), now()
    FROM observations o
    JOIN indicators i ON i.id = o.indicator_id
    WHERE i.code = p_indicator
      AND o.province_id IS NOT NULL
      AND o.observed_at BETWEEN p_from AND p_to
    GROUP BY o.province_id, o.indicator_id
    ON CONFLICT (province_id, indicator_id, period_start, period_end) DO UPDATE
        SET mean = EXCLUDED.mean, min = EXCLUDED.min, max = EXCLUDED.max,
            count = EXCLUDED.count, computed_at = now();
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END $$;

-- ---------------------------------------------------------------------
-- 6. Vistas para o painel e para os smoke tests
-- ---------------------------------------------------------------------

-- Estatisticas por provincia com unidade e fonte (contrato da API)
CREATE VIEW v_province_stats AS
SELECT p.code AS province_code, p.name AS province_name,
       i.code AS indicator_code, i.unit, i.source,
       s.period_start, s.period_end, s.mean, s.min, s.max, s.count
FROM province_stats s
JOIN provinces  p ON p.id = s.province_id
JOIN indicators i ON i.id = s.indicator_id;

-- Verificacao de dados para o Go/No-Go (secao 13 do Plano de Implantacao)
CREATE VIEW v_data_validation AS
SELECT i.code AS indicator_code,
       count(o.id)                              AS observations,
       count(o.id) FILTER (WHERE o.value IS NULL) AS null_values,
       min(o.value)                             AS min_value,
       max(o.value)                             AS max_value,
       count(o.id) FILTER (
           WHERE o.value < i.valid_min OR o.value > i.valid_max) AS out_of_range,
       count(DISTINCT o.province_id)            AS provinces_covered,
       min(o.observed_at)                       AS first_date,
       max(o.observed_at)                       AS last_date
FROM indicators i
LEFT JOIN observations o ON o.indicator_id = i.id
GROUP BY i.code;

-- Estado operacional resumido (ultima ingestao, ultima release, ultimo backup restaurado)
CREATE VIEW v_ops_status AS
SELECT
    (SELECT status || ' @ ' || started_at::text
       FROM ingestion_runs ORDER BY started_at DESC LIMIT 1)            AS last_ingestion,
    (SELECT version_tag || ' (' || environment || ', ' || status || ')'
       FROM releases ORDER BY deployed_at DESC LIMIT 1)                 AS last_release,
    (SELECT file_name FROM backup_log
       WHERE restore_tested ORDER BY restore_tested_at DESC LIMIT 1)    AS last_restored_backup;

-- ---------------------------------------------------------------------
-- 7. Dados iniciais (indicadores)
-- ---------------------------------------------------------------------

INSERT INTO indicators (code, name, unit, source, description, valid_min, valid_max) VALUES
 ('ndvi',       'Indice de vegetacao (NDVI)', 'indice (-1 a 1)', 'NASA Earthdata',
  'NDVI = (NIR - RED) / (NIR + RED); NaN quando NIR e RED sao zero (guardado como NULL).', -1, 1),
 ('population', 'Populacao',                  'habitantes',      'NASA SEDAC (GPW)',
  'Populacao em grelha; nao negativa.', 0, NULL);

-- As provincias (21) carregam-se a partir dos limites administrativos (GeoJSON/Shapefile,
-- EPSG:4326), por exemplo: ogr2ogr -f PostgreSQL PG:"dbname=earthpulse" limites.geojson ...

-- ---------------------------------------------------------------------
-- 8. Papeis e privilegios minimos
--    API: so leitura | Ingestao: escrita nas tabelas de dados e operacao
-- ---------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'earthpulse_api') THEN
        CREATE ROLE earthpulse_api NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'earthpulse_ingest') THEN
        CREATE ROLE earthpulse_ingest NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO earthpulse_api, earthpulse_ingest;

-- API: apenas SELECT
GRANT SELECT ON indicators, datasets, provinces, observations, province_stats,
                v_province_stats, v_data_validation
      TO earthpulse_api;
GRANT SELECT ON ingestion_runs, releases TO earthpulse_api;      -- para /health e estado
GRANT EXECUTE ON FUNCTION get_province_by_point(DOUBLE PRECISION, DOUBLE PRECISION),
                          get_point_series(TEXT, DOUBLE PRECISION, DOUBLE PRECISION, DATE, DATE, DOUBLE PRECISION)
      TO earthpulse_api;

-- Ingestao: leitura e escrita nos dados e no registo operacional
GRANT SELECT, INSERT, UPDATE, DELETE ON indicators, datasets, provinces, observations,
                                        province_stats, ingestion_runs, data_quality_checks
      TO earthpulse_ingest;
GRANT SELECT ON v_province_stats, v_data_validation, v_ops_status TO earthpulse_ingest;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO earthpulse_ingest;
GRANT EXECUTE ON FUNCTION refresh_province_stats(TEXT, DATE, DATE) TO earthpulse_ingest;

COMMIT;

-- ---------------------------------------------------------------------
-- 9. Depois de executar (fora deste ficheiro; nunca commitar palavras-passe):
--
--   ALTER ROLE earthpulse_api    LOGIN PASSWORD :'API_DB_PASSWORD';
--   ALTER ROLE earthpulse_ingest LOGIN PASSWORD :'INGEST_DB_PASSWORD';
--
--   -- DATABASE_URL da API deve usar earthpulse_api (so leitura);
--   -- a ingestao usa earthpulse_ingest.
--   -- Em producao, nao publicar a porta 5432.
--
-- Smoke tests (Plano de Implantacao, seccao 8):
--   SELECT * FROM get_province_by_point(-8.84, 13.23);
--   SELECT * FROM get_point_series('ndvi', -8.84, 13.23, '2025-01-01', '2025-12-31');
--   SELECT * FROM v_data_validation;
-- ---------------------------------------------------------------------
