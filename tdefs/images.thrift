namespace py images

/* Simple exception type */
exception Exception
{
    1: string msg
}

exception ImageNotFound {
    1: string msg,
    2: i32 image_id
}

struct Image {
    1: optional i32 id,
    2: optional i32 size,
    3: optional string vhash,
    4: optional string source_page_url,
    5: optional string source_url,
    6: optional double downloaded_at,
    7: optional string shahash,
    8: optional string data,
    9: optional i32 xdim,
    10: optional i32 ydim
}

service Images {
    Image get_image (1: i32 id)
    throws (1: Exception ex);

    list<Image> get_images_since
        (1: i32 image_id, 2: double timestamp, 3: i32 offset, 4: i32 limit)
    throws (1: Exception ex);

    list<Image> search (1: string source_blog_url,
                              2: double since_timestamp,
                              3: double before_timestamp,
                              4: list<i32> ids,
                              5: string source_url)
    throws (1: Exception ex);

    Image set_image (1: Image image)
    throws (1: Exception ex);

    bool delete_image (1: i32 image_id)
    throws (1: Exception ex);

    Image add_image (1: Image image)
    throws (1: Exception ex);

}
