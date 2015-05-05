from distop import rpc

def pow(ctx, esp, data):
	client = rpc.get_client('pyclass')
	client.cast(ctx, 'pow', esp=esp, data=data)