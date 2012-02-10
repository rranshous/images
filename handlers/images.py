from tgen.images import Images, ttypes as o
from lib.blobby import Blobby, o as bo
from lib.discovery import connect

from redis import Redis
import time
from lib.imgcompare.avg import average_hash
from cStringIO import StringIO
from PIL import Image

class ImagesHandler(object):
    def __init__(self, redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.rc = Redis(redis_host)

        # redis keys

        # incr this for the next image id
        # images:next_id = next_id

        # all the images for the given sha
        # images:datainstances:<shahash> = (ids)

        # timestamp of when image was added
        # images:ids:timestamps = sorted (ids,timestamp)

        # all the image ids for the page
        # images:page_ids:<page_url> (ids)

        # last time an image was added from page
        # images:pages:timestamps = sorted (url,timestamp)

        # images meta data
        # images:id = {}

    def _image_to_dict(self, image):
        data = {}
        ignored_attrs = ['data']
        for attrs in image.thrift_spec[1:]:
            attr = attrs[2]
            if attr in ignored_attrs:
                continue
            v = getattr(image,attr)
            if v is not None:
                data[attr] = v
        return data

    def _dict_to_image(self, data):
        image = o.Image()
        for attrs in image.thrift_spec[1:]:
            attr = attrs[2]
            v = data.get(attr)
            if v is not None:
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

    def _delete_from_redis(self, image):

        # make these a transaction
        pipe = self.rc.pipeline()

        # remove it from the id set
        pipe.zrem('images:ids:timestamps',image.id)

        # remove it's hash
        pipe.delete('images:%s' % image.id)

        # decriment the count for it's image data
        pipe.srem('images:datainstances:%s' % image.shahash,
                     image.id)

        # remove image from the page's id set
        if image.source_page_url:
            pipe.zrem('images:page_ids:%s' % image.source_page_url,
                      image.id)

        # make it happen
        pipe.execute()

        return True

    def _save_to_redis(self, image):

        # make these a transaction
        pipe = self.rc.pipeline()

        # if our image doesn't have an id, set it up w/ one
        if not image.id:
            print 'got new image: %s' % image.shahash
            image.id = self.rc.incr('images:next_id')
            pipe.sadd('images:datainstances:%s' % image.shahash,
                         image.id)

        # check and see if we used to have a different shahash
        old_shahash = self.rc.hget('images:%s' % image.id,'shahash')
        if old_shahash != image.shahash:
            # remove our id from the old shahash tracker
            pipe.srem('images:datainstances:%s' % old_shahash,
                         image.id)
            # add it to the new tracker
            pipe.sadd('images:datainstances:%s' % image.shahash,
                         image.id)


        # update / set our timestamp
        da = 0.0
        if image.downloaded_at:
            da = image.downloaded_at
        else:
            da = time.time()
        pipe.zadd('images:ids:timestamps',da,image.id)

        # add this image to the page's id set
        if image.source_page_url:
            pipe.zadd('images:page_ids:%s' % image.source_page_url,
                      da, image.id)

            # update our last scrape time for the page
            pipe.zadd('images:pages:timestamps',
                      image.id, image.source_page_url)

        # take our image and make a dict
        image_data = self._image_to_dict(image)

        # set our data to redis
        key = 'images:%s' % image.id
        pipe.hmset(key,image_data)

        # execute our pipe
        pipe.execute()

        return image

    def _get_from_redis(self, image_id):
        # if the image id is in the id set than pull it's details
        if self.rc.zrank('images:ids:timestamps',image_id) is not None:
            # get the image data from redis
            key = 'images:%s' % image_id
            image_data = self.rc.hgetall(key)
            if not image_data:
                print 'redis had no image data'
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
        """ returns Image for given id or blank Image """

        # see if we have an image
        image = self._get_from_redis(image_id)

        if not image:
            raise o.ImageNotFound('Could not get image', image_id)

        # pull the actual image data
        self._populate_image_data(image)

        return image

    def add_image(self, image):
        """ like set but if we already have this image from this
            page we're not going to add it again. will also
            fill out image stats (size, dimension) """

        # we're only for new images, no i'ds allowed
        # if u want to set an id by hand use set_image
        if image.id:
            raise o.Exception('Can not add image with id')

        if not image.data:
            raise o.Exception('Image must have data')

        if not image.source_page_url:
            raise o.Exception('Image must have source page url')

        # update it's stats
        image = self.populate_image_stats(image)

        # only add the image if we haven't seen it beforeQ
        # if we've seen it before there will be an id which
        # the set of images w/ this data and from this page share
        ids = self.rc.sinter('images:datainstance:%s' % image.shahash,
                             'images:page_ids:%s' % image.source_page_url)


        # we don't need to continue
        # we'll return back their original msg, w/o the id set
        if found:
            print 'image already exists, not setting'
            return image

        # so the image appears to be new, good for it
        return self.set_image(image)

    def set_image(self, image):
        """ sets image data, returns image """

        # would be better if we only saved if it didn't exist
        if image.data:
            # save the images data
            self._set_image_data(image)

        # could be an update, could be new
        image = self._save_to_redis(image)

        return image

    def delete_image(self, image_id):
        """ removes an image """

        # get it's image obj
        try:
            image = self.get_image(image_id)
        except o.ImageNotFound, ex:
            return False

        # delete the redis data
        self._delete_from_redis(image)

        # see if we need to remove the image data
        if self.rc.scard('images:datainstances:%s' % image.shahash) == 0:
            # no more images w/ the same data, remove image data
            with connect(Blobby) as c:
                c.delete_data(image.shahash)

        # and we're done!
        return True


    def get_images_since(self, image_id=None, timestamp=None,
                               limit=10, offset=0):
        """ returns list of tublr images or blank list which were
            added after given image id or timestamp """

        if image_id:

            # figure out what the current id is and than grab
            # our sorted set by index assuming that all ids
            # contain an image
            next_id = self.rc.get('tumblrimages:next_id')

            # how far from the end is the id given
            d = int(next_id) - image_id

            # starting back where we think this image is to + limit
            ids = self.rc.zrange('tumblrimages:ids:timestamps',-d,-d + limit)

        elif timestamp:

            print 'from timestamp: %s' % timestamp

            # get ids from our sorted set by it's weight (aka timestamp)
            ids = self.rc.zrangebyscore('tumblrimages:ids:timestamps',
                                        timestamp,'+inf')

        # page ids
        ids = ids[limit:offset]

        # return images for each ID
        images = map(self._get_from_redis,ids)

        # populate image data
        map(self._populate_image_data,images)

        return images

    def search(self, source_blog_url=None, since_timestamp=None,
                     before_timestamp=None, ids=[], source_url=None):
        """ returns list of  images, searches passed on passed params """
        pass

    def populate_image_stats(self, image):
        """ returns a Image w/ image data + stats filled
            out """
        ti = image
        image_data = ti.data
        if not ti.data:
            return ti
        ti.size = len(image_data)
        try:
            with connect(Blobby) as c:
                ti.shahash = c.get_data_bhash(image_data)
        except o.Exception, ex:
            raise o.Exception('oException getting shahash: %s' % ex.msg)
        except Exception, ex:
            raise o.Exception('Exception getting shahash: %s' % ex)

        try:
            b = StringIO(image_data)
            img = Image.open(b)
        except Exception, ex:
            raise o.Exception('Exception getting PIL img: %s' % ex)
        try:
            ti.xdim, ti.ydim = img.size
        except Exception, ex:
            raise o.Exception('Exception getting dimensions: %s' % ex)
        try:
            ti.vhash = str(average_hash(img))
        except Exception, ex:
            raise o.Exception('Exception getting vhash: %s' % ex)

        return ti

def run():
    from run_services import serve_service
    serve_service(Images, ImagesHandler())
