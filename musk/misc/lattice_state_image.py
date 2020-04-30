import math
import logging
from PIL import Image, ImageDraw


class LatticeStateImage:

    mode = "1"

    cell_width = None
    cell_height = None

    """
        Class representing an image frame. 
        Can be used in isolation or as part of an animation.

        Coordinates:
            x, y - variables always representing pixel positions, 
            i.e. distance measured in pixels from the 
            left, top boundaries, respectively.

            i, j - variables always representing lattice positions, 
            i.e i-th row, j-th column of the state matrix. 

    """

    def __init__(self, lattice, image_width=512, image_height=512):
        self.lattice = lattice
        self.image_width = image_width
        self.image_height = image_height
        self.logger = logging.getLogger("musk.lattice_state_image")

    def get_new_image(self):
        # Create PIL image instance
        # (color = None prevents the image from being initialized)
        return Image.new(self.mode, (self.image_width, self.image_height), color=None)

    def get_draw_context(self, pil_image):
        # Create drawing context on pil image
        return ImageDraw.Draw(pil_image)

    def calculate_cell_size(self):
        lattice_size = self.lattice.get_size()
        cell_width, cell_height = (
            self.image_width / lattice_size,
            self.image_height / lattice_size,
        )
        if not (cell_width.is_integer() and cell_height.is_integer()):
            self.logger.warning(
                "Cell width and/or cell height are not integers."
                "This might cause artifacts in the rendered image."
            )
        return cell_width, cell_height

    def get_cell_size(self):
        # Computes the size of a lattice cell in ImageFrame scale
        if not (self.cell_width and self.cell_height):
            self.cell_width, self.cell_height = self.calculate_cell_size()

        return self.cell_width, self.cell_height

    def get_lattice_coordinates_from_frame_coordinates(self, x, y):
        # Maps frame coordinates to lattice coordinates

        cell_width, cell_height = self.get_cell_size()
        i, j = math.floor(y / cell_height), math.floor(x / cell_width)
        return i, j

    def get_color_for_coordinates(self, x, y):
        # Given coordinates (x, y) representing
        # pixel distance from left and top corner,
        # respectively, return 0 for white and 1 for black

        i, j = self.get_lattice_coordinates_from_frame_coordinates(x, y)
        state = self.lattice.get_state_at_node(i, j)
        return int(not state)  # Invert 1<->0

    def get_color_iterator(self):
        # Returns an iterator whose elements are sequential colors
        # in the final image frame

        for x in range(self.image_width):
            for y in range(self.image_height):
                color = self.get_color_for_coordinates(x, y)
                yield color

    def draw_lattice(self, pil_image):
        # Draws the lattice to PILs Image object

        colors = self.get_color_iterator()

        # TODO - this removes the benefit of using generators, however, putdata requires sequences.
        colors = list(colors)

        return pil_image.putdata(colors)

    def save(self, file_path):

        pil_image = self.get_new_image()
        # pil_draw = self.get_draw_context(pil_image)

        # Renders the points
        self.draw_lattice(pil_image)

        return pil_image.save(file_path)
