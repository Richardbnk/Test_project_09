import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.ingest import ingest_data
from pipeline.clean import clean_data
from pipeline.analyze import analyze_earnings


def main():
    """
    Run the complete taxi analytics pipeline.
    """
    print("\nNYC TAXI ANALYTICS PIPELINE\n")
    
    # stage 1: load raw data
    print("\nSTAGE 1: INGEST")
    ingest_data()
    
    # stage 2: clean and enrich
    print("\nSTAGE 2: CLEAN")
    clean_data()
    
    # stage 3: analyze earnings
    print("\nSTAGE 3: ANALYZE")
    analyze_earnings()
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()
