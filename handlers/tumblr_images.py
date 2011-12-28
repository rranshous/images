from tgen.tumblrimages import TumblrImages, ttypes as o
from lib.blobby import Blobby, o as bo
from lib.discovery import connect

class TumblrImagesHandler(object):
    def __init__(self, redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.rc = Redis(redis_host)

        ## we are going to store the image data in redis using
        ## the id as the main key

        # tumblrimages:next_id = next_id

        # tumblrimages:ids: = [ids]
        # tumblrimages:ids:timestamps = sorted (ids,timestamp)
        # tumblrimages:blog_urls = ['urls']
        # tumblrimages:<blog_url>:blogimages = [ids]

        # tumblrimages:id = {}

    def _image_to_dict(self, image):
        data = {}
        for attrs in image.thrift_spec:
            attr = attrs[2]
            data[attr] = getattr(image,attr)
        return data

    def _dict_to_image(self, data):
        image = o.TumblrImage()
        for attrs in image.thrift_spec:
            attr = attrs[2]
            v = data.get(attr)
            if v:
                # we might need to update the value
                # type, since all values come back
                # from redis as strings
                attr_type = attrs[1]

                # float
                if attr_type == 4:
                    setattr(image,attr,float(v))
                # int
                elif attr_type == 8:
                    setattr(image,attr,int(v))
                else:
                    setattr(image,attr,v)
        return image

    def _save_to_redis(self, image):
        # if our image doesn't have an id, set it up w/ one
        if not image.id:
            image.id = self.rc.incr('tumblrimages:next_id')

        # if we know the source blog add in our entries
        # for those sets
        if image.source_blog_url:
            self.rc.sadd('tumblrimages:blog_urls',image.source_blog_url)
            self.rc.sadd('tumblrimages:%s:blogimages'%image.source_blog_url,
                         image_id)

        # add our image's id to the set of image ids
        self.rc.rpush('tumblrimages:ids',image.id)
        self.rc.zadd('tumblrimages:ids:timestamps',
                     image.id,image.downloaded_at)

        # take our image and make a dict
        image_data = self._image_to_dict(image)

        # set our data to redis
        key = 'tumblrimages:'+image.id
        self.rc.hmset(key,image_data)

        return image

    def _get_from_redis(self, image_id):
        # if the image id is in the id set than pull it's details
        if self.rc.sismember('tumblrimages:ids',image_id):
            # get the image data from redis
            key = 'tumblrimages:%s' % image_id
            image_data = self.rc.hgetall(key)
            if not image_data:
                return None
            image = self._dict_to_image(image_data)
            return image

        return None

    def _populate_image_data(self, image):
        if not image.shahash:
            return None
        with connect(Blobby) as c:
            image.data = c.get_data(image.shahash)
        return image

    def _set_image_data(self, image):
        if image.data is not None:
            with connect(Blobby) as c:
                image.shahash = c.set_data(image.data)
        return image

    def get_image(self, image_id):
        """ returns TumblrImage for given id or blank TumblrImage """

        # see if we have an image
        image = self._get_from_redis(image_id)

        if not image:
            raise TumblrImageNotFound(image_id, 'Could not get image')

        # pull the actual image data
        self._populate_image_data(image)

        return image

    def set_image(self, tumblr_image):
        """ sets tumblr image data, returns tumblr image """

        if tumblr_image.data and not tumblr_image.shahash:
            # save the images data
            self._set_image_data(image)

        # could be an update, could be new
        image = self._save_to_redis(tumblr_image)

        return image

    def get_images_since(self, image_id=None, timestamp=None):
        """ returns list of tublr images or blank list which were
            added after given image id or timestamp """

        if image_id:

            # we want all this to happen w/o changes, start transaction
            self.rc.multi()

            # find the index of the id we're given
            i = self.rc.lindex('tumblrimages:ids',image_id)

            # get all the members of the list from the id's index to the end
            ids = self.rc.lrange(i,-1)

        elif timestamp:

            # get ids from our sorted set by it's weight (aka timestamp)
            ids = self.rc.zrangebyscore('tumblrimages:ids:timestamps',
                                        timestamp,'+inf')

        # return images for each ID
        images = map(self._get_from_redis,ids)

        # populate image data
        map(self._populate_image_data,images)

        return images

    def search(self, source_blog_url=None, since_timestamp=None,
                     before_timestamp=None, ids=[], source_url=None):
        """ returns list of tumblr images, searches passed on passed params """
        pass

