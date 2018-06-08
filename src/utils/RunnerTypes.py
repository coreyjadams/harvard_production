from GalleryRunner import GalleryRunner
from ArtNextRunner import ArtNextRunner
from LarsoftRunner import LarsoftRunner
from ICRunner      import ICRunner

class RunnerTypes(dict):

    def __init__(self, **kwargs):
        super(RunnerTypes, self).__init__(kwargs)
        self['larsoft'] = LarsoftRunner
        self['gallery'] = GalleryRunner
        self['artnext'] = ArtNextRunner
        self['ic']      = ICRunner