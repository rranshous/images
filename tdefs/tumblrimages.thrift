namespace py tumblrimages

/* Simple exception type */
exception Exception
{
    1: string msg
}

exception TumblrImageNotFound {
    1: string msg,
    2: i32 image_id
}

struct TumblrImage {
    1: optional i32 id,
    2: optional i32 size,
    3: optional list<i32> dimensions,
    4: optional string vhash,
    5: optional string source_blog_url,
    6: optional string source_url,
    7: optional double downloaded_at,
    8: optional string shahash,
    9: optional string data
}

service TumblrImages {
    TumblrImage get_image (1: i32 id)
    throws (1: Exception ex);

    list<TumblrImage> get_images_since (1: i32 image_id, 2: double timestamp)
    throws (1: Exception ex);

    list<TumblrImage> search (1: string source_blog_url,
                              2: double since_timestamp,
                              3: double before_timestamp,
                              4: list<i32> ids,
                              5: string source_url)
    throws (1: Exception ex);

    TumblrImage set_image (1: TumblrImage image)
    throws (1: Exception ex);

    bool delete_image (1: i32 image_id)
    throws (1: Exception ex);
}
