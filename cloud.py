from wordcloud import WordCloud
import matplotlib.pyplot as plt

text = "es es es es es es es found that in certain cases (eg. with Spyder having plt.ion(): interactive mode = On) the figure is always shown. I work around this by forcing the closing of the figure window in my giant loop, so I do"

wordcloud = WordCloud(max_font_size=200).generate(text)
fig = plt.figure()
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
fig.savefig('images/foo.png')