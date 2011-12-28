from tgen.tumblrimages import TumblrImages, ttypes as o
from lib.blobby import Blobby, o as bo
from lib.discovery import connect

from redis import Redis
from lib.imgcompare.avg import average_hash
from cStringIO import StringIO
from PIL import Image

class TumblrImagesHandler(object):
    def __init__(self, redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.rc = Redis(redis_host)

        ## we are going to store the image data in redis using
        ## the id as the main key

        # tumblrimages:next_id = next_id
        # tumblrimages:datainstances:<shahash> = (ids)
        # tumblrimages:ids:timestamps = sorted (ids,timestamp)
        # tumblrimages:blog_urls = ['urls']
        # tumblrimages:<blog_url>:blogimages = (ids)

        # tumblrimages:id = {}

    def _image_to_dict(self, image):
        data = {}
        for attrs in image.thrift_spec[1:]:
            attr = attrs[2]
            v = getattr(image,attr)
            if v is not None:
                data[attr] = v
        return data

    def _dict_to_image(self, data):
        image = o.TumblrImage()
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

        # remove it from the blog images set
        self.rc.srem('tumblrimages:%s:blogimages'%image.source_blog_url,
                     image.id)

        # remove it from the id set
        self.rc.zrem('tumblrimages:ids:timestamps',image.id)

        # remove it's hash
        self.rc.delete('tumblrimages:%s' % image.id)

        # decriment the count for it's image data
        self.rc.srem('tumblrimages:datainstances:%s' % image.shahash,
                     image.id)

        return True

    def _save_to_redis(self, image):
        # if our image doesn't have an id, set it up w/ one
        if not image.id:
            image.id = self.rc.incr('tumblrimages:next_id')
            self.rc.sadd('tumblrimages:datainstances:%s' % image.shahash,
                         image.id)

        # check and see if we used to have a different shahash
        old_shahash = self.rc.hget('tumblrimages:%s' % image.id,'shahash')
        if old_shahash != image.shahash:
            # remove our id from the old shahash tracker
            self.rc.srem('tumblrimages:datainstances:%s' % old_shahash,
                         image.id)
            # add it to the new tracker
            self.rc.sadd('tumblrimages:datainstances:%s' % image.shahash,
                         image.id)

        # if we know the source blog add in our entries
        # for those sets
        if image.source_blog_url:
            self.rc.sadd('tumblrimages:blog_urls',image.source_blog_url)
            self.rc.sadd('tumblrimages:%s:blogimages'%image.source_blog_url,
                         image.id)

        # update / set our timestamp
        da = 0.0
        if image.downloaded_at:
            da = images.downloaded_at
        self.rc.zadd('tumblrimages:ids:timestamps',image.id,da)

        # take our image and make a dict
        # TODO: if this fails than all the operations we did above
        #       will still have gone through but we will have failed
        #       ... =/
        image_data = self._image_to_dict(image)

        # set our data to redis
        key = 'tumblrimages:%s' % image.id
        self.rc.hmset(key,image_data)

        return image

    def _get_from_redis(self, image_id):
        # if the image id is in the id set than pull it's details
        if self.rc.zrank('tumblrimages:ids:timestamps',image_id) is not None:
            # get the image data from redis
            key = 'tumblrimages:%s' % image_id
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
        """ returns TumblrImage for given id or blank TumblrImage """

        # see if we have an image
        image = self._get_from_redis(image_id)

        if not image:
            raise o.TumblrImageNotFound('Could not get image', image_id)

        # pull the actual image data
        self._populate_image_data(image)

        return image

    def add_image(self, image):
        """ like set but if we already have this image on this
            blog we're not going to add it again. will also
            fill out image stats (size, dimension) """

        # we're only for new images, no i'ds allowed
        # if u want to set an id by hand use set_image
        if image.id:
            raise o.Exception('Can not add image with id')

        if not image.data
            raise o.Exception('Image must have data')

        # update it's stats
        image = self.populate_image_stats(image)

        # check for a matching bhash on this blog
        # get ids which are both in the bhash's set
        # and also in the blog id set. get set intersection
        i = self.rc.sinter('tumblrimages:datainstances:%s' % image.shahash,
                           'tumblrimages:%s:blogimages' % image.source_blog_url)

        # if we get back anything than we already have this image
        # (image data) from this blog, we don't need to continue
        # we'll return back their original msg, w/o the id set
        if i:
            print 'image exists: %s' % i
            return image

        # so the image appears to be new, good for it
        return self.set_image(image)

    def set_image(self, image):
        """ sets tumblr image data, returns tumblr image """

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
        except o.TumblrImageNotFound, ex:
            return False

        # delete the redis data
        self._delete_from_redis(image)

        # see if we need to remove the image data
        if self.rc.scard('tumblrimages:datainstances:%s' % image.shahash) == 0:
            # no more images w/ the same data, remove image data
            with connect(Blobby) as c:
                c.delete_data(image.shahash)

        # and we're done!
        return True


    def get_images_since(self, image_id=None, timestamp=None):
        """ returns list of tublr images or blank list which were
            added after given image id or timestamp """

        if image_id:

            # get the timestamp of the image
            timestamp = self.rc.hget('tumblrimages:%s' % image_id, 'timestamp')
            if timestamp:
                timestamp = float(timestamp)

        if not timestamp:
            return []

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

    def populate_image_stats(self, image):
        """ returns a TumblrImage w/ image data + stats filled
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
    serve_service(TumblrImages, TumblrImagesHandler())
