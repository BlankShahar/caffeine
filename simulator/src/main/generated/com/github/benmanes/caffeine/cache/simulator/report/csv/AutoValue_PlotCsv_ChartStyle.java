package com.github.benmanes.caffeine.cache.simulator.report.csv;

import java.awt.Color;
import java.awt.Font;
import javax.annotation.processing.Generated;
import org.jfree.chart.ui.RectangleInsets;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PlotCsv_ChartStyle extends PlotCsv.ChartStyle {

  private final RectangleInsets axisOffset;

  private final Color title;

  private final Color subtitle;

  private final Color background;

  private final Color axisLine;

  private final Color axisLabel;

  private final Color gridLine;

  private final Color gridBand;

  private final Color legend;

  private final Color label;

  private final Font extraLargeFont;

  private final Font regularFont;

  private final Font largeFont;

  private final float brightness;

  private final float saturation;

  private final float alpha;

  private AutoValue_PlotCsv_ChartStyle(
      RectangleInsets axisOffset,
      Color title,
      Color subtitle,
      Color background,
      Color axisLine,
      Color axisLabel,
      Color gridLine,
      Color gridBand,
      Color legend,
      Color label,
      Font extraLargeFont,
      Font regularFont,
      Font largeFont,
      float brightness,
      float saturation,
      float alpha) {
    this.axisOffset = axisOffset;
    this.title = title;
    this.subtitle = subtitle;
    this.background = background;
    this.axisLine = axisLine;
    this.axisLabel = axisLabel;
    this.gridLine = gridLine;
    this.gridBand = gridBand;
    this.legend = legend;
    this.label = label;
    this.extraLargeFont = extraLargeFont;
    this.regularFont = regularFont;
    this.largeFont = largeFont;
    this.brightness = brightness;
    this.saturation = saturation;
    this.alpha = alpha;
  }

  @Override
  RectangleInsets axisOffset() {
    return axisOffset;
  }

  @Override
  Color title() {
    return title;
  }

  @Override
  Color subtitle() {
    return subtitle;
  }

  @Override
  Color background() {
    return background;
  }

  @Override
  Color axisLine() {
    return axisLine;
  }

  @Override
  Color axisLabel() {
    return axisLabel;
  }

  @Override
  Color gridLine() {
    return gridLine;
  }

  @Override
  Color gridBand() {
    return gridBand;
  }

  @Override
  Color legend() {
    return legend;
  }

  @Override
  Color label() {
    return label;
  }

  @Override
  Font extraLargeFont() {
    return extraLargeFont;
  }

  @Override
  Font regularFont() {
    return regularFont;
  }

  @Override
  Font largeFont() {
    return largeFont;
  }

  @Override
  float brightness() {
    return brightness;
  }

  @Override
  float saturation() {
    return saturation;
  }

  @Override
  float alpha() {
    return alpha;
  }

  @Override
  public String toString() {
    return "ChartStyle{"
        + "axisOffset=" + axisOffset + ", "
        + "title=" + title + ", "
        + "subtitle=" + subtitle + ", "
        + "background=" + background + ", "
        + "axisLine=" + axisLine + ", "
        + "axisLabel=" + axisLabel + ", "
        + "gridLine=" + gridLine + ", "
        + "gridBand=" + gridBand + ", "
        + "legend=" + legend + ", "
        + "label=" + label + ", "
        + "extraLargeFont=" + extraLargeFont + ", "
        + "regularFont=" + regularFont + ", "
        + "largeFont=" + largeFont + ", "
        + "brightness=" + brightness + ", "
        + "saturation=" + saturation + ", "
        + "alpha=" + alpha
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PlotCsv.ChartStyle) {
      PlotCsv.ChartStyle that = (PlotCsv.ChartStyle) o;
      return this.axisOffset.equals(that.axisOffset())
          && this.title.equals(that.title())
          && this.subtitle.equals(that.subtitle())
          && this.background.equals(that.background())
          && this.axisLine.equals(that.axisLine())
          && this.axisLabel.equals(that.axisLabel())
          && this.gridLine.equals(that.gridLine())
          && this.gridBand.equals(that.gridBand())
          && this.legend.equals(that.legend())
          && this.label.equals(that.label())
          && this.extraLargeFont.equals(that.extraLargeFont())
          && this.regularFont.equals(that.regularFont())
          && this.largeFont.equals(that.largeFont())
          && Float.floatToIntBits(this.brightness) == Float.floatToIntBits(that.brightness())
          && Float.floatToIntBits(this.saturation) == Float.floatToIntBits(that.saturation())
          && Float.floatToIntBits(this.alpha) == Float.floatToIntBits(that.alpha());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= axisOffset.hashCode();
    h$ *= 1000003;
    h$ ^= title.hashCode();
    h$ *= 1000003;
    h$ ^= subtitle.hashCode();
    h$ *= 1000003;
    h$ ^= background.hashCode();
    h$ *= 1000003;
    h$ ^= axisLine.hashCode();
    h$ *= 1000003;
    h$ ^= axisLabel.hashCode();
    h$ *= 1000003;
    h$ ^= gridLine.hashCode();
    h$ *= 1000003;
    h$ ^= gridBand.hashCode();
    h$ *= 1000003;
    h$ ^= legend.hashCode();
    h$ *= 1000003;
    h$ ^= label.hashCode();
    h$ *= 1000003;
    h$ ^= extraLargeFont.hashCode();
    h$ *= 1000003;
    h$ ^= regularFont.hashCode();
    h$ *= 1000003;
    h$ ^= largeFont.hashCode();
    h$ *= 1000003;
    h$ ^= Float.floatToIntBits(brightness);
    h$ *= 1000003;
    h$ ^= Float.floatToIntBits(saturation);
    h$ *= 1000003;
    h$ ^= Float.floatToIntBits(alpha);
    return h$;
  }

  static final class Builder extends PlotCsv.ChartStyle.Builder {
    private RectangleInsets axisOffset;
    private Color title;
    private Color subtitle;
    private Color background;
    private Color axisLine;
    private Color axisLabel;
    private Color gridLine;
    private Color gridBand;
    private Color legend;
    private Color label;
    private Font extraLargeFont;
    private Font regularFont;
    private Font largeFont;
    private float brightness;
    private float saturation;
    private float alpha;
    private byte set$0;
    Builder() {
    }
    @Override
    PlotCsv.ChartStyle.Builder axisOffset(RectangleInsets axisOffset) {
      if (axisOffset == null) {
        throw new NullPointerException("Null axisOffset");
      }
      this.axisOffset = axisOffset;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder title(Color title) {
      if (title == null) {
        throw new NullPointerException("Null title");
      }
      this.title = title;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder subtitle(Color subtitle) {
      if (subtitle == null) {
        throw new NullPointerException("Null subtitle");
      }
      this.subtitle = subtitle;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder background(Color background) {
      if (background == null) {
        throw new NullPointerException("Null background");
      }
      this.background = background;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder axisLine(Color axisLine) {
      if (axisLine == null) {
        throw new NullPointerException("Null axisLine");
      }
      this.axisLine = axisLine;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder axisLabel(Color axisLabel) {
      if (axisLabel == null) {
        throw new NullPointerException("Null axisLabel");
      }
      this.axisLabel = axisLabel;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder gridLine(Color gridLine) {
      if (gridLine == null) {
        throw new NullPointerException("Null gridLine");
      }
      this.gridLine = gridLine;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder gridBand(Color gridBand) {
      if (gridBand == null) {
        throw new NullPointerException("Null gridBand");
      }
      this.gridBand = gridBand;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder legend(Color legend) {
      if (legend == null) {
        throw new NullPointerException("Null legend");
      }
      this.legend = legend;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder label(Color label) {
      if (label == null) {
        throw new NullPointerException("Null label");
      }
      this.label = label;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder extraLargeFont(Font extraLargeFont) {
      if (extraLargeFont == null) {
        throw new NullPointerException("Null extraLargeFont");
      }
      this.extraLargeFont = extraLargeFont;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder regularFont(Font regularFont) {
      if (regularFont == null) {
        throw new NullPointerException("Null regularFont");
      }
      this.regularFont = regularFont;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder largeFont(Font largeFont) {
      if (largeFont == null) {
        throw new NullPointerException("Null largeFont");
      }
      this.largeFont = largeFont;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder brightness(float brightness) {
      this.brightness = brightness;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder saturation(float saturation) {
      this.saturation = saturation;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    PlotCsv.ChartStyle.Builder alpha(float alpha) {
      this.alpha = alpha;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    PlotCsv.ChartStyle build() {
      if (set$0 != 7
          || this.axisOffset == null
          || this.title == null
          || this.subtitle == null
          || this.background == null
          || this.axisLine == null
          || this.axisLabel == null
          || this.gridLine == null
          || this.gridBand == null
          || this.legend == null
          || this.label == null
          || this.extraLargeFont == null
          || this.regularFont == null
          || this.largeFont == null) {
        StringBuilder missing = new StringBuilder();
        if (this.axisOffset == null) {
          missing.append(" axisOffset");
        }
        if (this.title == null) {
          missing.append(" title");
        }
        if (this.subtitle == null) {
          missing.append(" subtitle");
        }
        if (this.background == null) {
          missing.append(" background");
        }
        if (this.axisLine == null) {
          missing.append(" axisLine");
        }
        if (this.axisLabel == null) {
          missing.append(" axisLabel");
        }
        if (this.gridLine == null) {
          missing.append(" gridLine");
        }
        if (this.gridBand == null) {
          missing.append(" gridBand");
        }
        if (this.legend == null) {
          missing.append(" legend");
        }
        if (this.label == null) {
          missing.append(" label");
        }
        if (this.extraLargeFont == null) {
          missing.append(" extraLargeFont");
        }
        if (this.regularFont == null) {
          missing.append(" regularFont");
        }
        if (this.largeFont == null) {
          missing.append(" largeFont");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" brightness");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" saturation");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" alpha");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_PlotCsv_ChartStyle(
          this.axisOffset,
          this.title,
          this.subtitle,
          this.background,
          this.axisLine,
          this.axisLabel,
          this.gridLine,
          this.gridBand,
          this.legend,
          this.label,
          this.extraLargeFont,
          this.regularFont,
          this.largeFont,
          this.brightness,
          this.saturation,
          this.alpha);
    }
  }

}
