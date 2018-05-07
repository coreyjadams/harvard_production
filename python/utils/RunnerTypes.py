from GalleryRunner import GalleryRunner
from ArtNextRunner import ArtNextRunner
from LarsoftRunner import LarsoftRunner

class RunnerTypes(dict):

    def __init__(self, **kwargs):
        super(RunnerTypes, self).__init__(kwargs)
        self['larsoft'] = LarsoftRunner
        self['gallery'] = GalleryRunner
        self['artnext'] = ArtNextRunner