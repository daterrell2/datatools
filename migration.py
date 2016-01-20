class Migration(object):

	def __init__(self, src, dest, transforms=[lambda x: x])

		self.src = src
		self.dest = dest
		self.transforms = transforms

	def migrate(self):

		src_data = (t(self.src.read()) for t in transforms)

		return self.dest.write(src_data)
