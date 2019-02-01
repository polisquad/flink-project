import matplotlib.pyplot as plt
import os

data_path = os.path.dirname(os.path.realpath(__file__))
in_file = os.path.join(data_path, "in.csv")
out_file = os.path.join(data_path, "out.csv")


def parse_2d(file=out_file):
    with open(file) as f:
        lines = f.readlines()
        x1s = []
        x2s = []
        colors = []

        for line in lines:
            x1, x2, color = [float(x) for x in line.split(",")]
            x1s += [x1]
            x2s += [x2]
            colors += [color]

    return x1s, x2s, colors


def plot2d(x1s, x2s, colors):
    plt.scatter(x1s, x2s, s=[50 for _ in range(len(x1s))], c=colors, marker="o")
    plt.show()


x1s, x2s, colors = parse_2d(file=out_file)
plot2d(x1s, x2s, colors)

if __name__ == '__main__':
    parse_2d()
