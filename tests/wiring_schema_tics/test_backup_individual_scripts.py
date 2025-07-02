"""
Individual test cases for each backup script in wiring_schema_tics/sql/bak/
Tests multiple scenarios per script using real database transactions.
"""

import pytest
from dagstributor.wiring_schema_tics.ops import (
    wst_atp_bak_media_op,
    wst_atp_bak_training_op,
    wst_atp_bak_prediction_op,
    wst_atp_instantiate_media_op,
    wst_atp_instantiate_training_op,
    wst_atp_instantiate_prediction_op,
)
import datetime


class TestBakMediaScript:
    """Test cases for bak_media.sql"""
    
    def test_backup_empty_media_table(self, dagster_context, db_fixture):
        """Test backing up an empty media table"""
        # Setup: Ensure clean state and create media table
        from dagstributor.wiring_schema_tics.ops import wst_atp_drop_op
        
        # Drop any existing schemas
        wst_atp_drop_op(dagster_context)
        
        # Create fresh media table
        wst_atp_instantiate_media_op(dagster_context)
        
        # Verify media table exists and is empty before backup
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'atp' AND table_name = 'media'
                """)
                assert cursor.fetchone() is not None, "Media table should exist before backup"
                
                cursor.execute("SELECT COUNT(*) as count FROM atp.media")
                media_count = cursor.fetchone()['count']
                assert media_count == 0, f"Media table should be empty, found {media_count} rows"
        
        # Execute backup operation
        result = wst_atp_bak_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup infrastructure was created properly
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Find the actual data backup table (not metadata or column_mapping)
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'bak' 
                    AND table_name LIKE 'atp_media_%'
                    AND table_name NOT LIKE '%_metadata'
                    AND table_name NOT LIKE '%_column_mapping'
                    ORDER BY table_name DESC
                    LIMIT 1
                """)
                data_backup_table = cursor.fetchone()
                assert data_backup_table is not None, "Data backup table should be created"
                
                # Verify the actual data backup table has same count as source
                data_table_name = data_backup_table['table_name']
                cursor.execute(f"SELECT COUNT(*) as count FROM bak.{data_table_name}")
                backup_count = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM atp.media")
                media_count_final = cursor.fetchone()['count']
                
                assert backup_count == media_count_final, f"Backup table should have same row count as source: backup={backup_count}, source={media_count_final}"
                
                # Verify metadata table exists and has backup info
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'bak' 
                    AND table_name LIKE 'atp_media_%_metadata'
                """)
                metadata_table = cursor.fetchone()
                assert metadata_table is not None, "Metadata table should be created"
                
                # Verify column mapping table exists
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'bak' 
                    AND table_name LIKE 'atp_media_%_column_mapping'
                """)
                mapping_table = cursor.fetchone()
                assert mapping_table is not None, "Column mapping table should be created"
    
    def test_backup_media_with_data(self, dagster_context, db_fixture, fixture_loader):
        """Test backing up media table with actual data"""
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
                for item in media_data[:3]:  # Insert first 3 items
                    columns = list(item.keys())
                    values = list(item.values())
                    placeholders = ', '.join(['%s'] * len(values))
                    column_names = ', '.join(columns)
                    
                    cursor.execute(f"""
                        INSERT INTO atp.media ({column_names})
                        VALUES ({placeholders})
                    """, values)
                conn.commit()
        
        # Execute backup operation
        result = wst_atp_bak_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup contains same data
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.bak_media")
                assert cursor.fetchone()[0] == 3
                
                # Verify data integrity
                cursor.execute("""
                    SELECT hash, media_title, media_type 
                    FROM atp.bak_media 
                    ORDER BY hash
                """)
                backup_data = cursor.fetchall()
                assert len(backup_data) == 3
    
    def test_backup_media_overwrites_existing(self, dagster_context, db_fixture):
        """Test that backup overwrites existing backup table"""
        # Setup: Create media table with initial data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_media_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert initial data
                cursor.execute("""
                    INSERT INTO atp.media (hash, media_title, created_at, updated_at)
                    VALUES ('hash1', 'Title 1', NOW(), NOW())
                """)
                conn.commit()
        
        # First backup
        wst_atp_bak_media_op(dagster_context)
        
        # Add more data
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO atp.media (hash, media_title, created_at, updated_at)
                    VALUES ('hash2', 'Title 2', NOW(), NOW())
                """)
                conn.commit()
        
        # Second backup - should replace first backup
        result = wst_atp_bak_media_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup has all current data
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.bak_media")
                assert cursor.fetchone()[0] == 2
    
    def test_backup_media_preserves_all_columns(self, dagster_context, db_fixture):
        """Test that backup preserves all columns and data types"""
        # Setup: Create media table
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_media_op(dagster_context)
        
        # Execute backup operation
        wst_atp_bak_media_op(dagster_context)
        
        # Compare column structures
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get original columns
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' AND table_name = 'media'
                    ORDER BY ordinal_position
                """)
                original_columns = cursor.fetchall()
                
                # Get backup columns
                cursor.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_schema = 'atp' AND table_name = 'bak_media'
                    ORDER BY ordinal_position
                """)
                backup_columns = cursor.fetchall()
                
                # Should have same structure
                assert len(backup_columns) == len(original_columns)
                for orig, bak in zip(original_columns, backup_columns):
                    assert orig[0] == bak[0]  # column name
                    assert orig[1] == bak[1]  # data type


class TestBakTrainingScript:
    """Test cases for bak_training.sql"""
    
    def test_backup_empty_training_table(self, dagster_context, db_fixture):
        """Test backing up an empty training table"""
        # Setup: Create empty training table
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        # Execute backup operation
        result = wst_atp_bak_training_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup table exists and is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM atp.bak_training
                """)
                assert cursor.fetchone()[0] == 0
    
    def test_backup_training_with_boolean_data(self, dagster_context, db_fixture):
        """Test backing up training table with boolean is_good values"""
        # Setup: Create training table with data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert mix of good and bad training data
                cursor.execute("""
                    INSERT INTO atp.training (hash, is_good, created_at, updated_at)
                    VALUES 
                        ('good1', true, NOW(), NOW()),
                        ('bad1', false, NOW(), NOW()),
                        ('good2', true, NOW(), NOW()),
                        ('bad2', false, NOW(), NOW())
                """)
                conn.commit()
        
        # Execute backup operation
        result = wst_atp_bak_training_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup preserves boolean values
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, is_good 
                    FROM atp.bak_training 
                    ORDER BY hash
                """)
                results = cursor.fetchall()
                assert len(results) == 4
                
                # Check boolean values preserved
                good_count = sum(1 for r in results if r[1] is True)
                bad_count = sum(1 for r in results if r[1] is False)
                assert good_count == 2
                assert bad_count == 2
    
    def test_backup_training_with_timestamps(self, dagster_context, db_fixture):
        """Test that backup preserves timestamp precision"""
        # Setup: Create training table with specific timestamps
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_training_op(dagster_context)
        
        test_time = datetime.datetime(2024, 1, 15, 10, 30, 45, 123456)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO atp.training (hash, is_good, created_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                """, ('test_hash', True, test_time, test_time))
                conn.commit()
        
        # Execute backup operation
        wst_atp_bak_training_op(dagster_context)
        
        # Verify timestamps preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT created_at, updated_at 
                    FROM atp.bak_training 
                    WHERE hash = 'test_hash'
                """)
                result = cursor.fetchone()
                # Timestamps should be close (within a second due to timezone handling)
                assert abs((result[0] - test_time).total_seconds()) < 1
                assert abs((result[1] - test_time).total_seconds()) < 1


class TestBakPredictionScript:
    """Test cases for bak_prediction.sql"""
    
    def test_backup_empty_prediction_table(self, dagster_context, db_fixture):
        """Test backing up an empty prediction table"""
        # Setup: Create empty prediction table
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        # Execute backup operation
        result = wst_atp_bak_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup table exists and is empty
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.bak_prediction")
                assert cursor.fetchone()[0] == 0
    
    def test_backup_prediction_with_scores(self, dagster_context, db_fixture):
        """Test backing up prediction table with numeric scores"""
        # Setup: Create prediction table with data
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert predictions with various scores
                cursor.execute("""
                    INSERT INTO atp.prediction 
                    (hash, score, predicted_good, predicted_good_transformed, created_at, updated_at)
                    VALUES 
                        ('pred1', 0.95, true, true, NOW(), NOW()),
                        ('pred2', 0.30, false, false, NOW(), NOW()),
                        ('pred3', 0.75, true, true, NOW(), NOW()),
                        ('pred4', 0.15, false, false, NOW(), NOW())
                """)
                conn.commit()
        
        # Execute backup operation
        result = wst_atp_bak_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify backup preserves scores and predictions
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, score, predicted_good, predicted_good_transformed
                    FROM atp.bak_prediction 
                    ORDER BY score DESC
                """)
                results = cursor.fetchall()
                assert len(results) == 4
                
                # Verify scores are preserved with proper precision
                assert float(results[0][1]) == 0.95  # highest score
                assert float(results[3][1]) == 0.15  # lowest score
                
                # Verify boolean predictions match scores
                high_scores = [r for r in results if float(r[1]) > 0.5]
                low_scores = [r for r in results if float(r[1]) <= 0.5]
                
                assert all(r[2] is True for r in high_scores)  # predicted_good
                assert all(r[2] is False for r in low_scores)
    
    def test_backup_prediction_null_handling(self, dagster_context, db_fixture):
        """Test backing up prediction table with NULL values"""
        # Setup: Create prediction table with NULLs
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert predictions with NULL scores
                cursor.execute("""
                    INSERT INTO atp.prediction 
                    (hash, score, predicted_good, predicted_good_transformed, created_at, updated_at)
                    VALUES 
                        ('null_pred1', NULL, NULL, NULL, NOW(), NOW()),
                        ('null_pred2', 0.5, NULL, true, NOW(), NOW()),
                        ('null_pred3', NULL, false, NULL, NOW(), NOW())
                """)
                conn.commit()
        
        # Execute backup operation
        result = wst_atp_bak_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify NULLs are preserved
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT hash, score, predicted_good, predicted_good_transformed
                    FROM atp.bak_prediction 
                    WHERE hash LIKE 'null_pred%'
                    ORDER BY hash
                """)
                results = cursor.fetchall()
                assert len(results) == 3
                
                # Check NULL preservation
                assert results[0][1] is None  # score
                assert results[0][2] is None  # predicted_good
                assert results[0][3] is None  # predicted_good_transformed
                
                assert results[1][2] is None  # predicted_good can be NULL
                assert results[2][1] is None  # score can be NULL
    
    def test_backup_prediction_large_dataset(self, dagster_context, db_fixture):
        """Test backing up prediction table with many rows"""
        # Setup: Create prediction table with many rows
        with db_fixture.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS atp CASCADE")
        wst_atp_instantiate_prediction_op(dagster_context)
        
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert 1000 predictions
                for i in range(1000):
                    score = i / 1000.0
                    predicted = score > 0.5
                    cursor.execute("""
                        INSERT INTO atp.prediction 
                        (hash, score, predicted_good, predicted_good_transformed, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                    """, (f'pred_{i:04d}', score, predicted, predicted))
                conn.commit()
        
        # Execute backup operation
        result = wst_atp_bak_prediction_op(dagster_context)
        assert result.value["status"] == "success"
        
        # Verify all rows backed up
        with db_fixture.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM atp.bak_prediction")
                assert cursor.fetchone()[0] == 1000
                
                # Spot check some values
                cursor.execute("""
                    SELECT score FROM atp.bak_prediction 
                    WHERE hash IN ('pred_0250', 'pred_0500', 'pred_0750')
                    ORDER BY score
                """)
                scores = [float(r[0]) for r in cursor.fetchall()]
                assert scores == [0.25, 0.5, 0.75]