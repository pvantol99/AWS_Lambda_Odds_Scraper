import scraper
import aws_s3 as s3s


def handler(event#, context
    ):
    scr = scraper.OddsScraper()
    odds_list = scr.get_odds()
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
    file_ = 'swisslos_nba_'+dt_string
    s3s.upload_object(odds_list, event['bucket'], event['folder_path']+file_)
   #for url in urls:
     #   img_obj, img_hash = scr.get_in_memory_image(url, 'jpeg')
      #  files.append(img_hash)
      #  s3s.upload_object(img_obj, event['bucket'], 'swisslos_nba_'+dt_string)
    scr.close_connection()
    return "Successfully loaded swisslos odds to bucket {}. Folder path {} and file name {}.".format(event['bucket'],
                                                                                                  event['folder_path'],
                                                                                                  file_)
event = {'bucket':'swisslosbucket','folder_type':'/new'}
handler(event)