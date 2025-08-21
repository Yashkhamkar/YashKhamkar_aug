# test.py
from sqlmodel import Session, create_engine
from server.utils.helpers import (
    generate_report,
    save_csv_report,
)  # <- import your new functions

engine = create_engine(
    "postgresql://postgres.xsohyjzcebdkzpwffzcq:7pYvJZDA3jnWOHdg@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
)

with Session(engine) as session:
    print("Testing generate_report function")
    print("=" * 40)

    try:
        csv_data = generate_report(session, limit=25)  # test new function
        print("Function completed successfully!")

        # optionally save
        file_path = save_csv_report("test_report", csv_data)
        print(f"CSV saved to: {file_path}")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()
