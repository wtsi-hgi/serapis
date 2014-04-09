
#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################



from rest_framework.parsers import BaseParser
import zlib

#from Celery_Django_Prj import settings


class GZIPParser(BaseParser):
    """
    Plain text parser.
    """

    media_type = 'application/gzip'
    
    def parse(self, stream, media_type=None, parser_context=None):
        """
        Simply return a string representing the body of the request.
        """
        pass
#

##        decompressed_data=zlib.decompress(stream.read())
##        return decompressed_data
#        #return stream.read()
#
#        #str_object1 = open('compressed_file', 'rb').read()
#        
#        print "VARS -----------------", vars(stream)
#        print "STREAM FROM PARSE FCT: ", stream
#        parser_context = parser_context or {}
#        request = parser_context['request']
#        encoding = parser_context.get('encoding')
#        
#        print "REQUEST EXTRACTED:::::::::::::::::", request
#        print "VARS OF REQUEST::::::::::::::::::::::::::::", vars(request)
#        return zlib.decompress(request.get())
##        f = open('my_recovered_log_file', 'wb')
##        f.write(str_object2)
##        f.close()
#
#
##        from StringIO import StringIO
##        import gzip
##
##        buff = StringIO(stream.read())
##        deflatedContent = gzip.GzipFile(fileobj=buff)
##    
##        return deflatedContent.read()

