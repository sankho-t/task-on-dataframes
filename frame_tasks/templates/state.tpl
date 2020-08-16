{% extends "planner.html" %}

{% block current_vars %}
<div>
    <div class="d-flex p-3 flex-wrap">
        {% set dv_urls = dataview_urls|reverse|list %}
        {% for vars in state.vars|reverse %}
        {% set outline = "btn-outline-primary" if loop.index0 == 0 else "btn-outline-secondary" %}
        <a href="{{dv_urls[loop.index0]}}" class="build-together p-2 m-2 btn {{outline}}" style="margin-right: 1rem;">
            {% for var in vars %}
            {% if cols_colors[var] %}
                {% set style_var = "text-decoration: " + cols_colors[var] + " double overline; text-decoration-thickness: 2px" %}
            {% else %}
                {% set style_var = "" %}
            {% endif %}
            <span class="variable source-column" style="{{style_var}}">{{var|e}}</span>
            {% endfor %}
        </a>
        {% endfor %}
    </div>
</div>

{% endblock %}

{% block open_files %}

<ol class="open_files">
    {% for fname in state.open_files %}
    <li>{{ fname }}</li>
    {% endfor %}
</ol>

{% endblock %}

{% block actions %}
<ol class="actions list-group">
    {% for act in state.actions|reverse %}
    <li class="list-group-item">{% block action scoped %}{% endblock %}</li>
    {% endfor %}
</ol>
{% endblock %}