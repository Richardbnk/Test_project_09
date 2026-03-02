import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import query_to_dataframe, PROJECT_ID, DATASET_ID


def analyze_earnings():
    """
    Generate insights about driver earnings.
    """
    project_id = PROJECT_ID
    dataset_id = DATASET_ID
    
    # define all our analytical queries to answer the earnings question
    analysis_queries = {
        'earnings_by_zone': f"""
            -- which locations make the most revenue and what drivers earn there
            SELECT
                pickup_zone,
                pickup_borough,
                COUNT(*) as trips,
                ROUND(SUM(total_earnings), 2) as total_revenue,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(tip_amount), 2) as avg_tip
            FROM `{project_id}.{dataset_id}.clean_trips`
            WHERE pickup_zone IS NOT NULL
            GROUP BY pickup_zone, pickup_borough
            ORDER BY total_revenue DESC
            LIMIT 20
        """,
        
        'earnings_by_hour': f"""
            -- which hours of the day are most profitable
            SELECT
                pickup_hour,
                COUNT(*) as trips,
                ROUND(SUM(total_earnings), 2) as total_revenue,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(trip_duration_minutes), 2) as avg_duration_min
            FROM `{project_id}.{dataset_id}.clean_trips`
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """,
        
        'trip_distance_analysis': f"""
            -- how distance affects earnings per mile and which ranges are best
            SELECT
                CASE 
                    WHEN trip_distance <= 1 THEN '0-1 miles'
                    WHEN trip_distance <= 2 THEN '1-2 miles'
                    WHEN trip_distance <= 5 THEN '2-5 miles'
                    WHEN trip_distance <= 10 THEN '5-10 miles'
                    ELSE '10+ miles'
                END as distance_range,
                COUNT(*) as trips,
                ROUND(SUM(total_earnings), 2) as total_revenue,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(SUM(trip_distance), 2) as total_distance,
                ROUND(AVG(total_earnings) / AVG(trip_distance), 2) as revenue_per_mile
            FROM `{project_id}.{dataset_id}.clean_trips`
            GROUP BY distance_range
            ORDER BY 
                CASE 
                    WHEN distance_range = '0-1 miles' THEN 1
                    WHEN distance_range = '1-2 miles' THEN 2
                    WHEN distance_range = '2-5 miles' THEN 3
                    WHEN distance_range = '5-10 miles' THEN 4
                    ELSE 5
                END
        """,
        
        'payment_type_analysis': f"""
            -- how cash vs card affects tips and total earnings
            SELECT
                CASE 
                    WHEN payment_type = 1 THEN 'Credit Card'
                    WHEN payment_type = 2 THEN 'Cash'
                    WHEN payment_type = 3 THEN 'No Charge'
                    WHEN payment_type = 4 THEN 'Dispute'
                    ELSE 'Unknown'
                END as payment_method,
                COUNT(*) as trips,
                ROUND(SUM(total_earnings), 2) as total_revenue,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(AVG(tip_amount), 2) as avg_tip,
                ROUND(100 * AVG(tip_amount) / AVG(total_earnings), 2) as tip_percentage
            FROM `{project_id}.{dataset_id}.clean_trips`
            GROUP BY payment_type
            ORDER BY total_revenue DESC
        """,

        # creative angle: effective hourly rate by zone
        # accounts for trip duration to show where drivers actually earn the most per hour worked
        'hourly_rate_by_zone': f"""
            SELECT
                pickup_zone,
                pickup_borough,
                COUNT(*) as trips,
                ROUND(AVG(total_earnings), 2) as avg_fare,
                ROUND(AVG(trip_duration_minutes), 1) as avg_duration_min,
                ROUND(AVG(total_earnings) / (AVG(trip_duration_minutes) / 60), 2) as effective_hourly_rate,
                ROUND(AVG(tip_amount), 2) as avg_tip
            FROM `{project_id}.{dataset_id}.clean_trips`
            WHERE pickup_zone IS NOT NULL
                AND trip_duration_minutes BETWEEN 1 AND 120
            GROUP BY pickup_zone, pickup_borough
            HAVING COUNT(*) >= 1000
            ORDER BY effective_hourly_rate DESC
            LIMIT 15
        """,

        # creative angle: the "hidden gems" - routes that most drivers miss
        # pickup-dropoff combos with high earnings but low trip volume
        'underserved_routes': f"""
            SELECT
                pickup_zone,
                dropoff_zone,
                COUNT(*) as trips,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(AVG(tip_amount), 2) as avg_tip,
                ROUND(AVG(trip_distance), 1) as avg_miles,
                ROUND(AVG(total_earnings) / (AVG(trip_duration_minutes) / 60), 2) as hourly_rate
            FROM `{project_id}.{dataset_id}.clean_trips`
            WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL
                AND trip_duration_minutes BETWEEN 1 AND 120
            GROUP BY pickup_zone, dropoff_zone
            HAVING COUNT(*) BETWEEN 100 AND 2000
                AND AVG(total_earnings) > 30
            ORDER BY hourly_rate DESC
            LIMIT 15
        """,

        # creative angle: day of week patterns
        # shows which days are actually worth driving vs staying home
        'day_of_week_analysis': f"""
            SELECT
                FORMAT_DATE('%A', pickup_date) as day_name,
                EXTRACT(DAYOFWEEK FROM pickup_date) as day_num,
                COUNT(*) as trips,
                ROUND(SUM(total_earnings), 2) as total_revenue,
                ROUND(AVG(total_earnings), 2) as avg_earnings,
                ROUND(AVG(tip_amount), 2) as avg_tip,
                ROUND(AVG(trip_distance), 2) as avg_distance
            FROM `{project_id}.{dataset_id}.clean_trips`
            GROUP BY day_name, day_num
            ORDER BY day_num
        """,
    }
    
    # run each analysis and show results
    results = {}
    for name, sql in analysis_queries.items():
        print(f"\n{'='*60}")
        print(f"  {name.upper().replace('_', ' ')}")
        print(f"{'='*60}")
        df = query_to_dataframe(sql)
        print(df.to_string(index=False))
        results[name] = df
    
    print("\nAnalysis complete.")
    return results

# run the analysis when script is executed directly
if __name__ == '__main__':
    analyze_earnings()
