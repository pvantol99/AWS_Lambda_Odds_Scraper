import aws_s3 as s3s
import scraper as scraper
import supabase_odds as supabase
from datetime import datetime


def handler(event, context):
    scr = scraper.OddsScraper()
    odds_list = scr.get_odds(
        sport1=event["sport"],
        country1=event["country"],
        league1=event["league"],
    )
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
    scr.close_connection()

    results = []

    if event.get("output") == "supabase" or event.get("supabase"):
        if supabase.save_run(
            source="pinnacle",
            sport=event["sport"],
            country=event["country"],
            league=event["league"],
            odds_list=odds_list,
            scraped_at=now,
        ):
            results.append("Supabase")

    if event.get("bucket"):
        file_ = "pinnacle_" + event["league"] + "_" + dt_string
        folder = event.get("folder_path") or event.get("folder_type") or ""
        s3s.upload_object(
            [dt_string, odds_list],
            event["bucket"],
            folder + file_,
        )
        results.append("S3 bucket {} key {}".format(event["bucket"], folder + file_))

    if not results:
        return "No output configured: set event.output='supabase' and/or event.bucket for S3."

    return "Successfully saved pinnacle odds to: {}.".format(", ".join(results))