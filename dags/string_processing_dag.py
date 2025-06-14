"""String processing DAG for testing repository connection."""

import time
from dagster import asset, job, op, Definitions


@op
def generate_text():
    """Generate sample text data."""
    sample_texts = [
        "Hello World from Dagster",
        "Testing string operations",
        "Data processing pipeline",
        "Simple text transformation",
        "Repository connection test"
    ]
    print(f"Generated {len(sample_texts)} text samples")
    return sample_texts


@op
def transform_text(texts):
    """Transform text data."""
    transformed = []
    for text in texts:
        # Simple transformations
        upper_text = text.upper()
        word_count = len(text.split())
        char_count = len(text)
        
        transformed.append({
            "original": text,
            "uppercase": upper_text,
            "word_count": word_count,
            "char_count": char_count
        })
    
    print(f"Transformed {len(transformed)} text items")
    return transformed


@op
def analyze_text(transformed_texts):
    """Analyze the transformed text data."""
    total_words = sum(item["word_count"] for item in transformed_texts)
    total_chars = sum(item["char_count"] for item in transformed_texts)
    avg_words = total_words / len(transformed_texts)
    
    analysis = {
        "total_texts": len(transformed_texts),
        "total_words": total_words,
        "total_characters": total_chars,
        "average_words_per_text": avg_words
    }
    
    print(f"Text analysis: {analysis}")
    return analysis


@job
def string_processing_job():
    """String processing job."""
    texts = generate_text()
    transformed = transform_text(texts)
    analyze_text(transformed)


# Asset-based approach
@asset
def raw_messages():
    """Generate raw message data."""
    messages = [
        f"Message {i}: Current timestamp {int(time.time()) + i}"
        for i in range(1, 8)
    ]
    return messages


@asset
def processed_messages(raw_messages):
    """Process raw messages."""
    processed = []
    for msg in raw_messages:
        words = msg.split()
        processed.append({
            "message": msg,
            "word_count": len(words),
            "first_word": words[0] if words else "",
            "last_word": words[-1] if words else "",
            "contains_timestamp": "timestamp" in msg.lower()
        })
    
    print(f"Processed {len(processed)} messages")
    return processed


@asset
def message_summary(processed_messages):
    """Create summary of processed messages."""
    summary = {
        "total_messages": len(processed_messages),
        "messages_with_timestamp": sum(1 for msg in processed_messages if msg["contains_timestamp"]),
        "total_words": sum(msg["word_count"] for msg in processed_messages),
        "unique_first_words": len(set(msg["first_word"] for msg in processed_messages))
    }
    
    print(f"Message summary: {summary}")
    return summary


# Definitions for this module
defs = Definitions(
    assets=[raw_messages, processed_messages, message_summary],
    jobs=[string_processing_job],
)