{% extends "state.tpl" %}
{% block action %}

<div class="action row mb-3 align-self-center">
    <div class="callmap col">
        <ul class="list-group" style="text-align: right;">
            {% set multi_arg = act.callmap_flat()|unique(attribute=2)|list|length > 1 %}
            {% for _source_df_index, same_df in act.callmap_flat()|groupby(0) %}
            <li class="list-group-item">
                <ul class="">
                    {% for _, source_col, arg, var in same_df %}
                    <li class="">
                        {% if cols_colors[source_col] %}
                            {% set style_var = "text-decoration: " + cols_colors[source_col] + " double overline; text-decoration-thickness: 2px" %}
                        {% else %}
                            {% set style_var = "" %}
                        {% endif %}
                        
                        <span class="source-column" style="{{style_var}}">
                        {% if var.is_pat %}
                            {% set start_tag = "<span class='column-match' >" %}
                            {{ var.highlight_match(source_col|e, start_tag, "</span>")|safe }}
                        {% else %}
                            {{ source_col|e }}
                        {% endif %}
                        </span>
                        {% if multi_arg %}
                        <span class="argument">({{ arg }})</span>
                        {% endif %}
                    </li>
                    {% endfor %}
                </ul>
            </li>
            {% endfor %}
        </ul>
    </div>

    <div class="col-sm-2 align-self-center">
        {% if link is defined %}
        <a href="{{ link }}" class="btn btn-primary task-name">{{act.Task}}</a>
        {% else %}
        {{act.Task}}
        {% endif %}
    </div>

    <div class="returns col">
        <ul class="list-group">
            {% for _, item in act.returns_int()|groupby(0) %}
            <li class="single_position list-group-item">
                {% for _, dest in item %}
                <span class="variable">{{dest|e}}</span>
                {% endfor %}
            </li>
            {% endfor %}
        </ul>
    </div>
</div>
{% endblock %}