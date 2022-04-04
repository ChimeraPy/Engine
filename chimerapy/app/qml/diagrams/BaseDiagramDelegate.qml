import QtQuick 2.0
import QtQuick.Window 2.1

Component {

    Rectangle {
        id: groupRectangle
        anchors.left: parent.left
        anchors.right: parent.right
        height: entries.childrenRect.height + groupTitle.height
        color: "#44475a"

        // Title of group
        Text {
            id: groupTitle
            anchors.top: parent.top
            anchors.topMargin: 10
            anchors.left: parent.left
            anchors.right: parent.right
            text: qsTr(sort_by)
            font.pointSize: 16
            horizontalAlignment: Text.AlignLeft
            color: "white"
        }

        // Underline of the Title
        Rectangle {
            id: groupTitleUnderline
            height: 2
            anchors.top: groupTitle.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            color: "white"
        }

        Flow {
            id: entries
            anchors.top: groupTitleUnderline.bottom
            anchors.topMargin: 15
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            spacing: 15

            Repeater {
                model: group
                Rectangle {
                    id: groupRectangle
                    width: 0.45*Screen.width
                    height: width * (9/16)
                    color: "#44475a"

                    Loader {
                        source: switch(dtype) {
                            case "video": return Qt.resolvedUrl("VideoDelegate.qml")
                            case "image": return Qt.resolvedUrl("ImageDelegate.qml")
                        }
                        property int _height: groupRectangle.height
                        property int _width: groupRectangle.width
                     }
                }
            }
        }
    }
}
