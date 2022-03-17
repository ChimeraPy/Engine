import QtQuick 2.0
import QtQuick.Window 2.1

Component {

    Rectangle {
        id: groupRectangle
        anchors.left: parent.left
        anchors.right: parent.right
        height: entries.childrenRect.height + entryTitle.height
        color: "#44475a"

        // Title of dtype
        Text {
            id: entryTitle
            anchors.top: parent.top
            anchors.topMargin: 15
            anchors.left: parent.left
            anchors.right: parent.right
            text: qsTr(sort_by)
            font.pointSize: 16
            horizontalAlignment: Text.AlignLeft
        }

        // Underline of the Title
        Rectangle {
            id: entryTitleUnderline
            height: 2
            anchors.top: entryTitle.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            color: "black"
        }

        Flow {
            id: entries
            anchors.top: entryTitleUnderline.bottom
            anchors.topMargin: 15
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            spacing: 15

            Repeater {
                model: group
                Rectangle {
                    id: groupRectangle
                    width: 0.3*Screen.width
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
