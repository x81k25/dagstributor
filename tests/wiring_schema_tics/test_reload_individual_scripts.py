"""
Individual test cases for each reload script in wiring_schema_tics/sql/bak/
Tests multiple scenarios per script using real database transactions.
"""

import pytest
from dagstributor.wiring_schema_tics.ops import (
    wst_atp_reload_media_op,
    wst_atp_reload_training_op,
    wst_atp_reload_prediction_op,
    wst_atp_instantiate_media_op,
    wst_atp_instantiate_training_op,
    wst_atp_instantiate_prediction_op,
    wst_atp_bak_media_op,
    wst_atp_bak_training_op,
    wst_atp_bak_prediction_op,
)
import datetime


class TestReloadMediaScript:
    """Test cases for reload_media.sql"""
    
    def test_reload_media_from_empty_backup(self, dagster_context, db_fixture):
        """Test reloading media from empty backup table"""
        # Setup: Use proper ops to create environment
        from dagstributor.wiring_schema_tics.ops import wst_atp_drop_op
        
        # Start with clean state
        wst_atp_drop_op(dagster_context)
        
        # Create fresh media table
        wst_atp_instantiate_media_op(dagster_context)
        
        # Verify media table exists and is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM atp.media")
                media_count = cursor.fetchone()['count']
                assert media_count == 0, f"Media table should be empty, found {media_count} rows"
        
        # Create backup of empty table
        wst_atp_bak_media_op(dagster_context)
        
        # Verify backup exists and is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'bak' 
                    AND table_name LIKE 'atp_media_%'
                    AND table_name NOT LIKE '%_metadata'
                    AND table_name NOT LIKE '%_column_mapping'
                """)
                backup_table = cursor.fetchone()
                assert backup_table is not None, "Backup table should exist"
                
                backup_table_name = backup_table['table_name']
                cursor.execute(f"SELECT COUNT(*) as count FROM bak.{backup_table_name}")
                backup_count = cursor.fetchone()['count']
                assert backup_count == 0, f"Backup should be empty, found {backup_count} rows"
        
        # Clear media table to simulate data loss
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.media")
                conn.commit()
        
        # Execute reload operation
        result = wst_atp_reload_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify media table is still empty (since backup was empty)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM atp.media")
                final_count = cursor.fetchone()['count']
                assert final_count == 0, f"Reloaded table should be empty, found {final_count} rows"
    
    def test_reload_media_with_data(self, dagster_context, db_fixture, fixture_loader):
        """Test reloading media data from backup"""
        # Setup: Create media table with data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_media_op(dagster_context)
        
        # Insert sample data
        sample_data = fixture_loader.get_sample_data()
        media_data = sample_data.get("media", [])
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                for item in media_data[:5]:  # Insert first 5 items
                    columns = list(item.keys())
                    values = list(item.values())
                    placeholders = ', '.join(['%s'] * len(values))
                    column_names = ', '.join(columns)
                    
                    cursor.execute(f"""
                        INSERT INTO atp.media ({column_names})
                        VALUES ({placeholders})
                    """, values)
                conn.commit()
        
        # Create backup
        wst_atp_bak_media_op(dagster_context)
        
        # Clear media table
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.media")
                conn.commit()
        
        # Execute reload operation
        result = wst_atp_reload_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify data restored
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.media")
                assert cursor.fetchone()[0] == 5
                
                # Verify specific data
                cursor.execute("""
                    SELECT hash, media_title, media_type 
                    FROM atp.media 
                    ORDER BY hash
                    LIMIT 1
                """)
                restored = cursor.fetchone()
                assert restored is not None
    
    def test_reload_media_preserves_all_columns(self, dagster_context, db_fixture):
        """Test that reload preserves all column values correctly"""
        # Setup: Create media table with comprehensive data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_media_op(dagster_context)
        
        test_data = {
            'hash': 'test_hash_123',
            'media_type': 'movie',
            'media_title': 'Test Movie',
            'season': 1,
            'episode': 5,
            'release_year': 2024,
            'pipeline_status': 'filtered',
            'error_status': True,
            'error_condition': 'Test error',
            'rejection_status': 'quality',
            'rejection_reason': 'Low quality',
            'imdb_rating': 7.5,
            'tmdb_rating': 8.2,
            'runtime': 120,
            'created_at': datetime.datetime.now(),
            'updated_at': datetime.datetime.now()
        }
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                columns = list(test_data.keys())
                values = list(test_data.values())
                placeholders = ', '.join(['%s'] * len(values))
                column_names = ', '.join(columns)
                
                cursor.execute(f"""
                    INSERT INTO atp.media ({column_names})
                    VALUES ({placeholders})
                """, values)
                conn.commit()
        
        # Backup and clear
        wst_atp_bak_media_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.media")
                conn.commit()
        
        # Reload
        wst_atp_reload_media_op(dagster_context)
        
        # Verify all data preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT {', '.join(columns)}
                    FROM atp.media 
                    WHERE hash = %s
                """, ('test_hash_123',))
                restored = cursor.fetchone()
                
                # Check key values preserved
                assert restored[0] == 'test_hash_123'  # hash
                assert restored[1] == 'movie'  # media_type
                assert restored[2] == 'Test Movie'  # media_title
                assert restored[3] == 1  # season
                assert restored[4] == 5  # episode
                assert float(restored[11]) == 7.5  # imdb_rating
                assert float(restored[12]) == 8.2  # tmdb_rating
    
    def test_reload_media_handles_arrays(self, dagster_context, db_fixture):
        """Test that reload correctly handles array columns"""
        # Setup: Create media with array data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_media_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO atp.media 
                    (hash, media_title, genre, spoken_languages, production_companies, created_at, updated_at)
                    VALUES 
                    (%s, %s, %s, %s, %s, NOW(), NOW())
                """, (
                    'array_test',
                    'Array Test Movie',
                    ['Action', 'Sci-Fi', 'Drama'],
                    ['en', 'es', 'fr'],
                    ['Studio A', 'Studio B']
                ))
                conn.commit()
        
        # Backup and reload
        wst_atp_bak_media_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.media")
                conn.commit()
        wst_atp_reload_media_op(dagster_context)
        
        # Verify arrays preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT genre, spoken_languages, production_companies
                    FROM atp.media 
                    WHERE hash = 'array_test'
                """)
                result = cursor.fetchone()
                assert result[0] == ['Action', 'Sci-Fi', 'Drama']
                assert result[1] == ['en', 'es', 'fr']
                assert result[2] == ['Studio A', 'Studio B']


class TestReloadTrainingScript:
    """Test cases for reload_training.sql"""
    
    def test_reload_training_from_empty_backup(self, dagster_context, db_fixture):
        """Test reloading training from empty backup table"""
        # Setup: Create empty training and backup tables
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        wst_atp_bak_training_op(dagster_context)
        
        # Execute reload operation
        result = wst_atp_reload_training_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify training table is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.training")
                assert cursor.fetchone()[0] == 0
    
    def test_reload_training_with_boolean_values(self, dagster_context, db_fixture):
        """Test reloading training data preserves boolean is_good values"""
        # Setup: Create training table with mixed boolean data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        test_data = [
            ('hash1', True),
            ('hash2', False),
            ('hash3', True),
            ('hash4', False),
            ('hash5', None),  # Test NULL handling
        ]
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                for hash_val, is_good in test_data:
                    cursor.execute("""
                        INSERT INTO atp.training (hash, is_good, created_at, updated_at)
                        VALUES (%s, %s, NOW(), NOW())
                    """, (hash_val, is_good))
                conn.commit()
        
        # Backup, clear, and reload
        wst_atp_bak_training_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.training")
                conn.commit()
        wst_atp_reload_training_op(dagster_context)
        
        # Verify boolean values preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, is_good 
                    FROM atp.training 
                    ORDER BY hash
                """)
                results = cursor.fetchall()
                assert len(results) == 5
                
                # Check each value
                result_dict = {r[0]: r[1] for r in results}
                assert result_dict['hash1'] is True
                assert result_dict['hash2'] is False
                assert result_dict['hash3'] is True
                assert result_dict['hash4'] is False
                assert result_dict['hash5'] is None
    
    def test_reload_training_preserves_timestamps(self, dagster_context, db_fixture):
        """Test that reload preserves exact timestamp values"""
        # Setup: Create training with specific timestamps
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        # Use specific timestamps
        created_time = datetime.datetime(2024, 1, 1, 12, 0, 0)
        updated_time = datetime.datetime(2024, 1, 2, 14, 30, 0)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO atp.training (hash, is_good, created_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                """, ('timestamp_test', True, created_time, updated_time))
                conn.commit()
        
        # Backup and reload
        wst_atp_bak_training_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.training")
                conn.commit()
        wst_atp_reload_training_op(dagster_context)
        
        # Verify timestamps preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT created_at, updated_at 
                    FROM atp.training 
                    WHERE hash = 'timestamp_test'
                """)
                result = cursor.fetchone()
                # Allow small difference due to timezone handling
                assert abs((result[0] - created_time).total_seconds()) < 1
                assert abs((result[1] - updated_time).total_seconds()) < 1
    
    def test_reload_training_large_dataset(self, dagster_context, db_fixture):
        """Test reloading training table with many rows"""
        # Setup: Create training table with many rows
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        # Insert 500 training records
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(500):
                    is_good = i % 2 == 0  # Alternate good/bad
                    cursor.execute("""
                        INSERT INTO atp.training (hash, is_good, created_at, updated_at)
                        VALUES (%s, %s, NOW(), NOW())
                    """, (f'train_{i:04d}', is_good))
                conn.commit()
        
        # Backup, clear, and reload
        wst_atp_bak_training_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.training")
                conn.commit()
        wst_atp_reload_training_op(dagster_context)
        
        # Verify all rows restored
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.training")
                assert cursor.fetchone()[0] == 500
                
                # Verify good/bad distribution
                cursor.execute("SELECT COUNT(*) FROM atp.training WHERE is_good = true")
                assert cursor.fetchone()[0] == 250
                
                cursor.execute("SELECT COUNT(*) FROM atp.training WHERE is_good = false")
                assert cursor.fetchone()[0] == 250


class TestReloadPredictionScript:
    """Test cases for reload_prediction.sql"""
    
    def test_reload_prediction_from_empty_backup(self, dagster_context, db_fixture):
        """Test reloading prediction from empty backup table"""
        # Setup: Create empty prediction and backup tables
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        wst_atp_bak_prediction_op(dagster_context)
        
        # Execute reload operation
        result = wst_atp_reload_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify prediction table is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.prediction")
                assert cursor.fetchone()[0] == 0
    
    def test_reload_prediction_with_scores(self, dagster_context, db_fixture):
        """Test reloading prediction data with numeric scores"""
        # Setup: Create prediction table with score data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        test_predictions = [
            ('pred1', 0.99, True, True),
            ('pred2', 0.01, False, False),
            ('pred3', 0.50, True, False),
            ('pred4', None, None, None),  # Test NULL handling
        ]
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                for hash_val, score, pred_good, pred_trans in test_predictions:
                    cursor.execute("""
                        INSERT INTO atp.prediction 
                        (hash, score, predicted_good, predicted_good_transformed, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                    """, (hash_val, score, pred_good, pred_trans))
                conn.commit()
        
        # Backup, clear, and reload
        wst_atp_bak_prediction_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.prediction")
                conn.commit()
        wst_atp_reload_prediction_op(dagster_context)
        
        # Verify all data preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, score, predicted_good, predicted_good_transformed
                    FROM atp.prediction 
                    ORDER BY hash
                """)
                results = cursor.fetchall()
                assert len(results) == 4
                
                # Verify specific values
                result_dict = {r[0]: (r[1], r[2], r[3]) for r in results}
                assert float(result_dict['pred1'][0]) == 0.99
                assert result_dict['pred1'][1] is True
                assert result_dict['pred1'][2] is True
                
                assert float(result_dict['pred2'][0]) == 0.01
                assert result_dict['pred2'][1] is False
                assert result_dict['pred2'][2] is False
                
                assert result_dict['pred4'][0] is None
                assert result_dict['pred4'][1] is None
                assert result_dict['pred4'][2] is None
    
    def test_reload_prediction_precision(self, dagster_context, db_fixture):
        """Test that reload preserves numeric precision for scores"""
        # Setup: Create prediction with precise scores
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        # Test various precision levels
        test_scores = [
            ('prec1', 0.123456789),
            ('prec2', 0.999999999),
            ('prec3', 0.000000001),
            ('prec4', 0.5),
        ]
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                for hash_val, score in test_scores:
                    cursor.execute("""
                        INSERT INTO atp.prediction 
                        (hash, score, created_at, updated_at)
                        VALUES (%s, %s, NOW(), NOW())
                    """, (hash_val, score))
                conn.commit()
        
        # Backup and reload
        wst_atp_bak_prediction_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.prediction")
                conn.commit()
        wst_atp_reload_prediction_op(dagster_context)
        
        # Verify precision preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, score 
                    FROM atp.prediction 
                    WHERE hash LIKE 'prec%'
                    ORDER BY hash
                """)
                results = cursor.fetchall()
                
                # Check precision (allowing for floating point representation)
                for original in test_scores:
                    restored = next(r for r in results if r[0] == original[0])
                    assert abs(float(restored[1]) - original[1]) < 1e-9
    
    def test_reload_prediction_with_mixed_nulls(self, dagster_context, db_fixture):
        """Test reloading predictions with various NULL patterns"""
        # Setup: Create predictions with different NULL patterns
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Various NULL patterns
                cursor.execute("""
                    INSERT INTO atp.prediction 
                    (hash, score, predicted_good, predicted_good_transformed, created_at, updated_at)
                    VALUES 
                    ('null_all', NULL, NULL, NULL, NOW(), NOW()),
                    ('null_score', NULL, true, true, NOW(), NOW()),
                    ('null_pred', 0.75, NULL, true, NOW(), NOW()),
                    ('null_trans', 0.25, false, NULL, NOW(), NOW()),
                    ('no_nulls', 0.5, true, false, NOW(), NOW())
                """)
                conn.commit()
        
        # Backup and reload
        wst_atp_bak_prediction_op(dagster_context)
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE atp.prediction")
                conn.commit()
        wst_atp_reload_prediction_op(dagster_context)
        
        # Verify NULL patterns preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, 
                           score IS NULL as score_null,
                           predicted_good IS NULL as pred_null,
                           predicted_good_transformed IS NULL as trans_null
                    FROM atp.prediction 
                    ORDER BY hash
                """)
                results = {r[0]: (r[1], r[2], r[3]) for r in cursor.fetchall()}
                
                assert results['null_all'] == (True, True, True)
                assert results['null_score'] == (True, False, False)
                assert results['null_pred'] == (False, True, False)
                assert results['null_trans'] == (False, False, True)
                assert results['no_nulls'] == (False, False, False)