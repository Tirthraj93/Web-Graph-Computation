def write(path, out_tuple):
    with open(path, 'w') as f:
        for t in out_tuple:
            out = "{}\t{}\n".format(t[0], t[1])
            f.write(out)


def write_list(path, list):
    with open(path, 'w') as f:
        for item in list:
            out = "{}\n".format(item)
            f.write(out)


def read_file_to_list(path):
    list_data = []
    with open(path, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            list_data.append(line.strip())
    return list_data