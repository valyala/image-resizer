package image_resizer

import (
	"appengine"
	"appengine/memcache"
	"appengine/urlfetch"
	"bytes"
	"github.com/nfnt/resize"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"net/http"
	"strconv"

	"blobcache"
)

func init() {
	http.HandleFunc("/", serveHTTP)
}

func serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.RequestURI == "/favicon.ico" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	c := appengine.NewContext(r)
	imageUrl, width, height, resizeQuality := getImageParams(c, r)
	if len(imageUrl) == 0 || width < 0 || height < 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	img, format := loadImage(c, imageUrl)
	if img == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if width > 0 || height > 0 {
		if width == 0 {
			width = height
		} else if height == 0 {
			height = width
		}
		img = resize.Thumbnail(uint(width), uint(height), img, resizeQuality)
	}

	if !sendResponse(w, c, img, format, imageUrl) {
		// w.WriteHeader() is skipped intentionally here, since the response may be already partially created.
		return
	}
}

func sendResponse(w http.ResponseWriter, c appengine.Context, img image.Image, format, imageUrl string) bool {
	h := w.Header()
	switch format {
	case "gif":
		h.Set("Content-Type", "image/gif")
		if err := gif.Encode(w, img, nil); err != nil {
			c.Errorf("Cannot encode gif image from imageUrl=%v: %v", imageUrl, err)
			return false
		}
	case "jpeg":
		h.Set("Content-Type", "image/jpeg")
		if err := jpeg.Encode(w, img, nil); err != nil {
			c.Errorf("Cannot encode jpeg image from imageUrl=%v: %v", imageUrl, err)
			return false
		}
	case "png":
		h.Set("Content-Type", "image/png")
		if err := png.Encode(w, img); err != nil {
			c.Errorf("Cannot encode png image from imageUrl=%v: %v", imageUrl, err)
			return false
		}
	default:
		c.Errorf("Unsupported image format=%v for imageUrl=%v", format, imageUrl)
		return false
	}
	return true
}

func getImageParams(c appengine.Context, r *http.Request) (imageUrl string, width, height int, resizeQuality resize.InterpolationFunction) {
	imageUrl = r.FormValue("imageUrl")
	if len(imageUrl) == 0 {
		c.Errorf("Missing imageUrl request parameter")
		return
	}

	width = getImageDimension(c, r, "width")
	height = getImageDimension(c, r, "height")

	switch r.FormValue("resizeQuality") {
	case "0":
		resizeQuality = resize.NearestNeighbor
	case "1":
		resizeQuality = resize.Bilinear
	case "2":
		resizeQuality = resize.Bicubic
	case "3":
		resizeQuality = resize.MitchellNetravali
	case "4":
		resizeQuality = resize.Lanczos2
	case "5":
		resizeQuality = resize.Lanczos3
	default:
		resizeQuality = resize.Bilinear
	}

	return
}

func getImageDimension(c appengine.Context, r *http.Request, dimension string) int {
	v := r.FormValue(dimension)
	if len(v) == 0 {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		c.Errorf("Cannot parse %s=%v: %v", dimension, v, err)
		return -1
	}
	if n < 0 {
		c.Errorf("%s=%v must be positive", dimension, v)
		return -1
	}
	return n
}

func loadImage(c appengine.Context, imageUrl string) (img image.Image, format string) {
	item, err := blobcache.Get(c, imageUrl)
	if err == nil {
		if img, format, err = image.Decode(bytes.NewReader(item.Value)); err != nil {
			c.Errorf("Cannot parse image with imageUrl=%v obtained from memcache: %v. Fetching the image again from imageUrl", imageUrl, err)
			// return skipped intentionally
		} else {
			return
		}
	}

	client := urlfetch.Client(c)
	resp, err := client.Get(imageUrl)
	if err != nil {
		c.Errorf("Cannot load image from imageUrl=%v: %v", imageUrl, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		c.Errorf("Unexpected StatusCode=%d returned from imageUrl=%v", resp.StatusCode, imageUrl)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.Errorf("Error when reading image body from imageUrl=%v: %v", imageUrl, err)
		return
	}
	if img, format, err = image.Decode(bytes.NewReader(body)); err != nil {
		c.Errorf("Cannot parse image from imageUrl=%v: %v", imageUrl, err)
		return
	}

	item = &memcache.Item{
		Key: imageUrl,
		Value: body,
	}
	blobcache.Set(c, item)

	return img, format
}
