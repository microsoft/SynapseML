# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *
from mmlspark.TypeConversionUtils import generateTypeConverter, complexTypeConverter

@inherit_doc
class _BingImageSearch(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        aspect (object): Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.
        color (object): Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow
        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        count (object): The number of image results to return in the response. The actual number delivered may be less than requested.
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        freshness (object): Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        height (object): Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.
        imageContent (object): Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders
        imageType (object): Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.
        license (object): Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.
        maxFileSize (object): Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.
        maxHeight (object): Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
        maxWidth (object): Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
        minFileSize (object): Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.
        minHeight (object): Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
        minWidth (object): Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
        mkt (object): The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US
        offset (object): The zero-based offset that indicates the number of image results to skip before returning results
        outputCol (str): The name of the output column (default: [self.uid]_output)
        q (object): The user's search query string
        size (object): Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service (default: https://api.cognitive.microsoft.com/bing/v7.0/images/search)
        width (object): Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.
    """

    @keyword_only
    def __init__(self, aspect=None, color=None, concurrency=1, concurrentTimeout=100.0, count=None, errorCol=None, freshness=None, handler=None, height=None, imageContent=None, imageType=None, license=None, maxFileSize=None, maxHeight=None, maxWidth=None, minFileSize=None, minHeight=None, minWidth=None, mkt=None, offset=None, outputCol=None, q=None, size=None, subscriptionKey=None, timeout=60.0, url="https://api.cognitive.microsoft.com/bing/v7.0/images/search", width=None):
        super(_BingImageSearch, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.BingImageSearch")
        self._cache = {}
        self.aspect = Param(self, "aspect", "aspect: Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.")
        self.color = Param(self, "color", "color: Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow")
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.count = Param(self, "count", "count: The number of image results to return in the response. The actual number delivered may be less than requested.")
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.freshness = Param(self, "freshness", "freshness: Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.height = Param(self, "height", "height: Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.")
        self.imageContent = Param(self, "imageContent", "imageContent: Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders")
        self.imageType = Param(self, "imageType", "imageType: Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.")
        self.license = Param(self, "license", "license: Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.")
        self.maxFileSize = Param(self, "maxFileSize", "maxFileSize: Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.")
        self.maxHeight = Param(self, "maxHeight", "maxHeight: Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.")
        self.maxWidth = Param(self, "maxWidth", "maxWidth: Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.")
        self.minFileSize = Param(self, "minFileSize", "minFileSize: Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.")
        self.minHeight = Param(self, "minHeight", "minHeight: Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.")
        self.minWidth = Param(self, "minWidth", "minWidth: Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.")
        self.mkt = Param(self, "mkt", "mkt: The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US")
        self.offset = Param(self, "offset", "offset: The zero-based offset that indicates the number of image results to skip before returning results")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.q = Param(self, "q", "q: The user's search query string")
        self.size = Param(self, "size", "size: Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.")
        self.subscriptionKey = Param(self, "subscriptionKey", "subscriptionKey: the API key to use")
        self.timeout = Param(self, "timeout", "timeout: number of seconds to wait before closing the connection (default: 60.0)")
        self._setDefault(timeout=60.0)
        self.url = Param(self, "url", "url: Url of the service (default: https://api.cognitive.microsoft.com/bing/v7.0/images/search)")
        self._setDefault(url="https://api.cognitive.microsoft.com/bing/v7.0/images/search")
        self.width = Param(self, "width", "width: Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, aspect=None, color=None, concurrency=1, concurrentTimeout=100.0, count=None, errorCol=None, freshness=None, handler=None, height=None, imageContent=None, imageType=None, license=None, maxFileSize=None, maxHeight=None, maxWidth=None, minFileSize=None, minHeight=None, minWidth=None, mkt=None, offset=None, outputCol=None, q=None, size=None, subscriptionKey=None, timeout=60.0, url="https://api.cognitive.microsoft.com/bing/v7.0/images/search", width=None):
        """
        Set the (keyword only) parameters

        Args:

            aspect (object): Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.
            color (object): Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow
            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            count (object): The number of image results to return in the response. The actual number delivered may be less than requested.
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            freshness (object): Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            height (object): Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.
            imageContent (object): Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders
            imageType (object): Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.
            license (object): Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.
            maxFileSize (object): Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.
            maxHeight (object): Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
            maxWidth (object): Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
            minFileSize (object): Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.
            minHeight (object): Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
            minWidth (object): Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
            mkt (object): The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US
            offset (object): The zero-based offset that indicates the number of image results to skip before returning results
            outputCol (str): The name of the output column (default: [self.uid]_output)
            q (object): The user's search query string
            size (object): Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service (default: https://api.cognitive.microsoft.com/bing/v7.0/images/search)
            width (object): Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setAspect(self, value):
        """

        Args:

            aspect (object): Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.

        """
        self._java_obj = self._java_obj.setAspect(value)
        return self


    def setAspectCol(self, value):
        """

        Args:

            aspect (object): Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.

        """
        self._java_obj = self._java_obj.setAspectCol(value)
        return self




    def getAspect(self):
        """

        Returns:

            object: Filter images by the following aspect ratios: Square: Return images with standard aspect ratioWide: Return images with wide screen aspect ratioTall: Return images with tall aspect ratioAll: Do not filter by aspect. Specifying this value is the same as not specifying the aspect parameter.
        """
        return self._cache.get("aspect", None)


    def setColor(self, value):
        """

        Args:

            color (object): Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow

        """
        self._java_obj = self._java_obj.setColor(value)
        return self


    def setColorCol(self, value):
        """

        Args:

            color (object): Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow

        """
        self._java_obj = self._java_obj.setColorCol(value)
        return self




    def getColor(self):
        """

        Returns:

            object: Filter images by the following color options:ColorOnly: Return color imagesMonochrome: Return black and white imagesReturn images with one of the following dominant colors:Black,Blue,Brown,Gray,Green,Orange,Pink,Purple,Red,Teal,White,Yellow
        """
        return self._cache.get("color", None)


    def setConcurrency(self, value):
        """

        Args:

            concurrency (int): max number of concurrent calls (default: 1)

        """
        self._set(concurrency=value)
        return self


    def getConcurrency(self):
        """

        Returns:

            int: max number of concurrent calls (default: 1)
        """
        return self.getOrDefault(self.concurrency)


    def setConcurrentTimeout(self, value):
        """

        Args:

            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)

        """
        self._set(concurrentTimeout=value)
        return self


    def getConcurrentTimeout(self):
        """

        Returns:

            double: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        """
        return self.getOrDefault(self.concurrentTimeout)


    def setCount(self, value):
        """

        Args:

            count (object): The number of image results to return in the response. The actual number delivered may be less than requested.

        """
        self._java_obj = self._java_obj.setCount(value)
        return self


    def setCountCol(self, value):
        """

        Args:

            count (object): The number of image results to return in the response. The actual number delivered may be less than requested.

        """
        self._java_obj = self._java_obj.setCountCol(value)
        return self




    def getCount(self):
        """

        Returns:

            object: The number of image results to return in the response. The actual number delivered may be less than requested.
        """
        return self._cache.get("count", None)


    def setErrorCol(self, value):
        """

        Args:

            errorCol (str): column to hold http errors (default: [self.uid]_error)

        """
        self._set(errorCol=value)
        return self


    def getErrorCol(self):
        """

        Returns:

            str: column to hold http errors (default: [self.uid]_error)
        """
        return self.getOrDefault(self.errorCol)


    def setFreshness(self, value):
        """

        Args:

            freshness (object): Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates

        """
        self._java_obj = self._java_obj.setFreshness(value)
        return self


    def setFreshnessCol(self, value):
        """

        Args:

            freshness (object): Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates

        """
        self._java_obj = self._java_obj.setFreshnessCol(value)
        return self




    def getFreshness(self):
        """

        Returns:

            object: Filter images by the following discovery options:Day: Return images discovered by Bing within the last 24 hoursWeek: Return images discovered by Bing within the last 7 daysMonth: Return images discovered by Bing within the last 30 daysYear: Return images discovered within the last year2017-06-15..2018-06-15: Return images discovered within the specified range of dates
        """
        return self._cache.get("freshness", None)


    def setHandler(self, value):
        """

        Args:

            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))

        """
        self._set(handler=value)
        return self


    def getHandler(self):
        """

        Returns:

            object: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        """
        return self._cache.get("handler", None)


    def setHeight(self, value):
        """

        Args:

            height (object): Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.

        """
        self._java_obj = self._java_obj.setHeight(value)
        return self


    def setHeightCol(self, value):
        """

        Args:

            height (object): Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.

        """
        self._java_obj = self._java_obj.setHeightCol(value)
        return self




    def getHeight(self):
        """

        Returns:

            object: Filter images that have the specified height, in pixels.You may use this filter with the size filter to return small images that have a height of 150 pixels.
        """
        return self._cache.get("height", None)


    def setImageContent(self, value):
        """

        Args:

            imageContent (object): Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders

        """
        self._java_obj = self._java_obj.setImageContent(value)
        return self


    def setImageContentCol(self, value):
        """

        Args:

            imageContent (object): Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders

        """
        self._java_obj = self._java_obj.setImageContentCol(value)
        return self




    def getImageContent(self):
        """

        Returns:

            object: Filter images by the following content types:Face: Return images that show only a person's facePortrait: Return images that show only a person's head and shoulders
        """
        return self._cache.get("imageContent", None)


    def setImageType(self, value):
        """

        Args:

            imageType (object): Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.

        """
        self._java_obj = self._java_obj.setImageType(value)
        return self


    def setImageTypeCol(self, value):
        """

        Args:

            imageType (object): Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.

        """
        self._java_obj = self._java_obj.setImageTypeCol(value)
        return self




    def getImageType(self):
        """

        Returns:

            object: Filter images by the following image types:AnimatedGif: return animated gif imagesAnimatedGifHttps: return animated gif images that are from an https addressClipart: Return only clip art imagesLine: Return only line drawingsPhoto: Return only photographs (excluding line drawings, animated Gifs, and clip art)Shopping: Return only images that contain items where Bing knows of a merchant that is selling the items. This option is valid in the en-US market only. Transparent: Return only images with a transparent background.
        """
        return self._cache.get("imageType", None)


    def setLicense(self, value):
        """

        Args:

            license (object): Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.

        """
        self._java_obj = self._java_obj.setLicense(value)
        return self


    def setLicenseCol(self, value):
        """

        Args:

            license (object): Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.

        """
        self._java_obj = self._java_obj.setLicenseCol(value)
        return self




    def getLicense(self):
        """

        Returns:

            object: Filter images by the following license types:Any: Return images that are under any license type. The response doesn't include images that do not specify a license or the license is unknown.Public: Return images where the creator has waived their exclusive rights, to the fullest extent allowed by law.Share: Return images that may be shared with others. Changing or editing the image might not be allowed. Also, modifying, sharing, and using the image for commercial purposes might not be allowed. Typically, this option returns the most images.ShareCommercially: Return images that may be shared with others for personal or commercial purposes. Changing or editing the image might not be allowed.Modify: Return images that may be modified, shared, and used. Changing or editing the image might not be allowed. Modifying, sharing, and using the image for commercial purposes might not be allowed. ModifyCommercially: Return images that may be modified, shared, and used for personal or commercial purposes. Typically, this option returns the fewest images.All: Do not filter by license type. Specifying this value is the same as not specifying the license parameter. For more information about these license types, see Filter Images By License Type.
        """
        return self._cache.get("license", None)


    def setMaxFileSize(self, value):
        """

        Args:

            maxFileSize (object): Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.

        """
        self._java_obj = self._java_obj.setMaxFileSize(value)
        return self


    def setMaxFileSizeCol(self, value):
        """

        Args:

            maxFileSize (object): Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.

        """
        self._java_obj = self._java_obj.setMaxFileSizeCol(value)
        return self




    def getMaxFileSize(self):
        """

        Returns:

            object: Filter images that are less than or equal to the specified file size.The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly larger than the specified maximum.You may specify this filter and minFileSize to filter images within a range of file sizes.
        """
        return self._cache.get("maxFileSize", None)


    def setMaxHeight(self, value):
        """

        Args:

            maxHeight (object): Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMaxHeight(value)
        return self


    def setMaxHeightCol(self, value):
        """

        Args:

            maxHeight (object): Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMaxHeightCol(value)
        return self




    def getMaxHeight(self):
        """

        Returns:

            object: Filter images that have a height that is less than or equal to the specified height. Specify the height in pixels.You may specify this filter and minHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
        """
        return self._cache.get("maxHeight", None)


    def setMaxWidth(self, value):
        """

        Args:

            maxWidth (object): Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMaxWidth(value)
        return self


    def setMaxWidthCol(self, value):
        """

        Args:

            maxWidth (object): Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMaxWidthCol(value)
        return self




    def getMaxWidth(self):
        """

        Returns:

            object: Filter images that have a width that is less than or equal to the specified width. Specify the width in pixels.You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
        """
        return self._cache.get("maxWidth", None)


    def setMinFileSize(self, value):
        """

        Args:

            minFileSize (object): Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.

        """
        self._java_obj = self._java_obj.setMinFileSize(value)
        return self


    def setMinFileSizeCol(self, value):
        """

        Args:

            minFileSize (object): Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.

        """
        self._java_obj = self._java_obj.setMinFileSizeCol(value)
        return self




    def getMinFileSize(self):
        """

        Returns:

            object: Filter images that are greater than or equal to the specified file size. The maximum file size that you may specify is 520,192 bytes. If you specify a larger value, the API uses 520,192. It is possible that the response may include images that are slightly smaller than the specified minimum. You may specify this filter and maxFileSize to filter images within a range of file sizes.
        """
        return self._cache.get("minFileSize", None)


    def setMinHeight(self, value):
        """

        Args:

            minHeight (object): Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMinHeight(value)
        return self


    def setMinHeightCol(self, value):
        """

        Args:

            minHeight (object): Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMinHeightCol(value)
        return self




    def getMinHeight(self):
        """

        Returns:

            object: Filter images that have a height that is greater than or equal to the specified height. Specify the height in pixels.You may specify this filter and maxHeight to filter images within a range of heights. This filter and the height filter are mutually exclusive.
        """
        return self._cache.get("minHeight", None)


    def setMinWidth(self, value):
        """

        Args:

            minWidth (object): Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMinWidth(value)
        return self


    def setMinWidthCol(self, value):
        """

        Args:

            minWidth (object): Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.

        """
        self._java_obj = self._java_obj.setMinWidthCol(value)
        return self




    def getMinWidth(self):
        """

        Returns:

            object: Filter images that have a width that is greater than or equal to the specified width. Specify the width in pixels. You may specify this filter and maxWidth to filter images within a range of widths. This filter and the width filter are mutually exclusive.
        """
        return self._cache.get("minWidth", None)


    def setMkt(self, value):
        """

        Args:

            mkt (object): The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US

        """
        self._java_obj = self._java_obj.setMkt(value)
        return self


    def setMktCol(self, value):
        """

        Args:

            mkt (object): The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US

        """
        self._java_obj = self._java_obj.setMktCol(value)
        return self




    def getMkt(self):
        """

        Returns:

            object: The market where the results come from. Typically, this is the country where the user is making the request from; however, it could be a different country if the user is not located in a country where Bing delivers results. The market must be in the form -. For example, en-US. Full list of supported markets: es-AR,en-AU,de-AT,nl-BE,fr-BE,pt-BR,en-CA,fr-CA,es-CL,da-DK,fi-FI,fr-FR,de-DE,zh-HK,en-IN,en-ID,en-IE,it-IT,ja-JP,ko-KR,en-MY,es-MX,nl-NL,en-NZ,no-NO,zh-CN,pl-PL,pt-PT,en-PH,ru-RU,ar-SA,en-ZA,es-ES,sv-SE,fr-CH,de-CH,zh-TW,tr-TR,en-GB,en-US,es-US
        """
        return self._cache.get("mkt", None)


    def setOffset(self, value):
        """

        Args:

            offset (object): The zero-based offset that indicates the number of image results to skip before returning results

        """
        self._java_obj = self._java_obj.setOffset(value)
        return self


    def setOffsetCol(self, value):
        """

        Args:

            offset (object): The zero-based offset that indicates the number of image results to skip before returning results

        """
        self._java_obj = self._java_obj.setOffsetCol(value)
        return self




    def getOffset(self):
        """

        Returns:

            object: The zero-based offset that indicates the number of image results to skip before returning results
        """
        return self._cache.get("offset", None)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)


    def setQ(self, value):
        """

        Args:

            q (object): The user's search query string

        """
        self._java_obj = self._java_obj.setQ(value)
        return self


    def setQCol(self, value):
        """

        Args:

            q (object): The user's search query string

        """
        self._java_obj = self._java_obj.setQCol(value)
        return self




    def getQ(self):
        """

        Returns:

            object: The user's search query string
        """
        return self._cache.get("q", None)


    def setSize(self, value):
        """

        Args:

            size (object): Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.

        """
        self._java_obj = self._java_obj.setSize(value)
        return self


    def setSizeCol(self, value):
        """

        Args:

            size (object): Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.

        """
        self._java_obj = self._java_obj.setSizeCol(value)
        return self




    def getSize(self):
        """

        Returns:

            object: Filter images by the following sizes:Small: Return images that are less than 200x200 pixelsMedium: Return images that are greater than or equal to 200x200 pixels but less than 500x500 pixelsLarge: Return images that are 500x500 pixels or largerWallpaper: Return wallpaper images.AllDo not filter by size. Specifying this value is the same as not specifying the size parameter.You may use this parameter along with the height or width parameters. For example, you may use height and size to request small images that are 150 pixels tall.
        """
        return self._cache.get("size", None)


    def setSubscriptionKey(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKey(value)
        return self


    def setSubscriptionKeyCol(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKeyCol(value)
        return self




    def getSubscriptionKey(self):
        """

        Returns:

            object: the API key to use
        """
        return self._cache.get("subscriptionKey", None)


    def setTimeout(self, value):
        """

        Args:

            timeout (double): number of seconds to wait before closing the connection (default: 60.0)

        """
        self._set(timeout=value)
        return self


    def getTimeout(self):
        """

        Returns:

            double: number of seconds to wait before closing the connection (default: 60.0)
        """
        return self.getOrDefault(self.timeout)


    def setUrl(self, value):
        """

        Args:

            url (str): Url of the service (default: https://api.cognitive.microsoft.com/bing/v7.0/images/search)

        """
        self._set(url=value)
        return self


    def getUrl(self):
        """

        Returns:

            str: Url of the service (default: https://api.cognitive.microsoft.com/bing/v7.0/images/search)
        """
        return self.getOrDefault(self.url)


    def setWidth(self, value):
        """

        Args:

            width (object): Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.

        """
        self._java_obj = self._java_obj.setWidth(value)
        return self


    def setWidthCol(self, value):
        """

        Args:

            width (object): Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.

        """
        self._java_obj = self._java_obj.setWidthCol(value)
        return self




    def getWidth(self):
        """

        Returns:

            object: Filter images that have the specified width, in pixels.You may use this filter with the size filter to return small images that have a width of 150 pixels.
        """
        return self._cache.get("width", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.BingImageSearch"

    @staticmethod
    def _from_java(java_stage):
        module_name=_BingImageSearch.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".BingImageSearch"
        return from_java(java_stage, module_name)
